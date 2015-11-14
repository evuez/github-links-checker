import aiohttp
import json
import logging
import asyncio
import sqlite3
from lxml import html
from time import time
from datetime import datetime
from settings import USR, PWD
from urllib.parse import urlparse


ISSUES = 'https://api.github.com/repos/evuez/github-links-checker/issues'

logging.basicConfig(level=logging.DEBUG)

repositories = 'https://api.github.com/repositories'

readmes = asyncio.Queue()
links = asyncio.Queue()
broken_links = asyncio.Queue()


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
            if repo['fork']:
                logging.info("Skipping %s: fork", repo['full_name'])
                continue

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
                yield from readmes.put((
                    repo['owner']['login'],
                    repo['html_url'],
                    readme
                ))
            else:
                logging.warning('No README found for %s', repo['url'])
        else:
            repositories = get_http_link(request.headers['link'], 'next')
            logging.debug("Updating endpoint to %s", repositories)


@asyncio.coroutine
def queue_links():
    while True:
        readme = yield from readmes.get()
        dom = html.fromstring(readme[2])
        for link in dom.xpath('//a/@href'):
            if link.startswith('#'):
                continue
            yield from links.put(readme[:2] + (link,))


@asyncio.coroutine
def process_links():
    while True:
        link = yield from links.get()
        try:
            request = yield from aiohttp.head(link[2])
            logging.info('%s returned a %d', link[2], request.status)
            if (request.status // 100) not in (4, 5):
                continue
            if request.status == 405:
                continue  # Method Not Allowed
            yield from broken_links.put(link + (request.status,))
            logging.warning('Found broken link: %s', link[2])
        except GeneratorExit:
            pass
        except Exception:
            pass
        finally:
            request.close()


@asyncio.coroutine
def report_links():
    db = sqlite3.connect('links.db')
    while True:
        link = yield from broken_links.get()

        issue = json.dumps({
            'title': "Found a broken link in {repo_short}!".format(repo_short=urlparse(link[1]).path[1:]),
            'body': """
@{owner}, there's a [broken link]({broken_link}) in the README of [one of your repositories]({repo}).
You should fix it so that users don't get confused while browsing it!

> A `HEAD` request was sent to {broken_link} and an `HTTP {http_status}` was returned.

*This is a bot-generated issue, reply to this issue if this link wasn't broken or if you fixed it so I can close it!*
            """.format(
                owner=link[0],
                repo=link[1],
                broken_link=link[2],
                http_status=link[3]
            ).strip(),
            'labels': [
                'broken-link',
            ]
        })

        r = yield from aiohttp.post(ISSUES, data=issue, auth=(USR, PWD))

        db.execute('insert into links values (?)', (link[2],))
        db.commit()

        yield from wait_if_required(r.headers)

        r.close()
    db.close()


def main():
    loop = asyncio.get_event_loop()
    tasks = [
        queue_readmes(),
        queue_links(),
        process_links(),
        process_links(),
        process_links(),
        report_links(),
    ]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


if __name__ == '__main__':
    main()
