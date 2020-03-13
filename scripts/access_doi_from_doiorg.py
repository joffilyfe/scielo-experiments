import os
import csv
import asyncio
import logging
from logging import config
import argparse

import aiohttp

SUB_DICT_CONFIG = {"level": "DEBUG", "handlers": ["file"], "propagate": False}

config.dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "%(asctime)s %(levelname)-5.5s : %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            }
        },
        "handlers": {
            "console": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "default",
                "stream": "ext://sys.stdout",
            },
        },
        "root": {"level": "DEBUG", "handlers": ["console"]},
    }
)

logger = logging.getLogger(__name__)


class Counter:
    def __init__(self, count=0):
        self.count = count

    def inc(self):
        self.count += 1

        if self.count % 100 == 0:
            logger.info("Quantidade de artigos processados: %s.", self.count)


async def write_csv(data, output):
    writer = csv.DictWriter(output, fieldnames=data.keys(), delimiter=";")
    writer.writerow(data)


async def access_doi_website(session, data, counter, output):
    """Acessa o site do doi.org. Utiliza a saída do script de extração do ArticleMeta"""

    pid, doi = data.strip().split(";")[:2]
    generic_doi = f"10.1590/{pid}"

    DOIs = {"doi": doi, "generic_doi": generic_doi}
    result = {
        "pid": pid,
        "doi": doi,
        "found_by_doi": 0,
        "found_by_generic_doi": 0,
        "redirect_url": None,
        "error": None,
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0"
    }

    for name, doi in DOIs.items():
        if doi is None or len(doi) == 0:
            continue

        try:
            async with session.get("https://doi.org/%s" % doi, headers=headers) as response:
                if response.status in (200, 301, 302) or response.status >= 500:
                    result["found_by_%s" % name] = 1
                    result["redirect_url"] = str(response.url)
                    result["doi"] = doi

                    if response.status >= 500:
                        result["error"] = "Status code %s" % response.status

                    break

                elif response.status == 404 and "doi.org" not in str(response.url):
                    result["found_by_%s" % name] = 1
                    result["redirect_url"] = str(response.url)
                    result["doi"] = doi
                    result["error"] = "Status code %s" % response.status

        except Exception as exc:
            logger.error("Could not access doi for %s", pid)
            result["error"] = str(exc)
            result["found_by_%s" % name] = 1
            result["doi"] = doi
            break

    await write_csv(result, output)
    counter.inc()


async def bound_fetch(session, pid, counter, sem, output):

    async with sem:
        await access_doi_website(session, pid, counter, output)


async def main():
    parser = argparse.ArgumentParser(description="Utilitário para verificação de DOI")
    parser.add_argument(
        "file",
        help="Arquivo resultado da extração dos DOIs do ArticleMeta",
        type=argparse.FileType("r"),
    )
    parser.add_argument(
        "output",
        help="Arquivo de saída com o resultado do processamento",
        type=argparse.FileType("a"),
    )
    parser.add_argument(
        "--limit",
        help="Limite de conexões abertas ao mesmo tempo.",
        type=int,
        default=10,
    )

    args = parser.parse_args()

    lines = args.file.readlines()
    logger.info("Quantidade de linhas %s", len(lines))
    sem = asyncio.Semaphore(args.limit)
    tasks = []

    async with aiohttp.ClientSession() as session:
        counter = Counter()

        for line in lines:
            line = line.strip()
            tasks.append(bound_fetch(session, line, counter, sem, args.output))

        if tasks:
            logger.info("Quantidade de tasks registradas: %s", len(tasks))
            await asyncio.wait(tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(main())
