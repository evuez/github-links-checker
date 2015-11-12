import aiohttp
import json
import re
import logging
import asyncio
import sqlite3
import traceback
from lxml import html
from time import time
from datetime import datetime
from base64 import b64decode
from settings import USR, PWD
from os.path import splitext
from collections import namedtuple




logging.basicConfig(level=logging.INFO)

repositories = 'https://api.github.com/repositories'

readmes = asyncio.Queue()
links = asyncio.Queue()


def get_http_link(link, rel):
    return next(l for l in link.split(',') if rel in l).split(';')[0][1:-1]


def wait_if_required(headers):
    if int(headers['X-RateLimit-Remaining']) > 2:
        return
    reset = int(headers['X-RateLimit-Reset'])
    while (reset - int(time())) > 0:
        logging.info(
            "Waiting until %s (%d minutes remaining)",
            datetime.fromtimestamp(reset).strftime('%Y-%m-%d %H:%M:%S'),
            (reset - int(time())) // 60
        )
        yield from asyncio.sleep(120)


@asyncio.coroutine
def queue_readmes():
    global repositories
    while True:
        request = yield from aiohttp.get(repositories, auth=(USR, PWD))
        repos = yield from request.text()

        for repo in json.loads(repos):
            r = yield from aiohttp.get(
                "{}/readme".format(repo['url']),
                auth=(USR, PWD),
                headers={'accept': 'application/vnd.github.3.html'}
            )

            logging.debug("Checking %s", repo['url'])
            logging.debug("Remaining: %s", r.headers['X-RateLimit-Remaining'])

            yield from wait_if_required(r.headers)

            readme = yield from r.text()
            if readme:
                yield from readmes.put(readme)
            else:
                logging.warning('No README found for %s', repo['url'])
        else:
            repositories = get_http_link(request.headers['link'], 'next')
            logging.debug("Updating endpoint to %s", repositories)


@asyncio.coroutine
def queue_links():
    while True:
        readme = yield from readmes.get()
        dom = html.fromstring(readme)
        for link in dom.xpath('//a/@href'):
            if link.startswith('#'):
                continue
            yield from links.put(link)


@asyncio.coroutine
def process_links():
    db = sqlite3.connect('links.db')
    while True:
        link = yield from links.get()
        try:
            request = yield from aiohttp.head(link)
            logging.info('%s returned a %d', link, request.status)
            if (request.status // 100) not in (4, 5):
                continue
            db.execute('insert into links values (?)', (link,))
            db.commit()
            logging.warning('Found broken link: %s', link)
        except GeneratorExit:
            pass
        except Exception:
            pass
        finally:
            request.close()
    db.close()


def main():
    loop = asyncio.get_event_loop()
    tasks = [
        queue_readmes(),
        queue_links(),
        process_links(),
        process_links(),
        process_links(),
    ]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


if __name__ == '__main__':
    main()
