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

        if self.count % 500 == 0:
            logger.info("Quantidade de artigos processados: %s.", self.count)


async def write_csv(data, output):
    writer = csv.DictWriter(output, fieldnames=data.keys(), delimiter=";")
    writer.writerow(data)


async def fetch_article_meta_doi(session, pid, collection, counter, output):
    """Busca pelo DOI do artigo no article meta"""

    url = f"http://articlemeta.scielo.org/api/v1/article/?collection=scl&code={pid}"

    data = {
        "pid": pid,
        "doi": None,
        "found": 0,
        "collection": collection,
        "error": None,
    }

    async with session.get(url) as response:
        try:
            json = await response.json()
            doi = json["doi"]
        except Exception as exc:
            data["error"] = str(exc)
        else:
            data["doi"] = doi
            data["found"] = 1

    await write_csv(data, output)
    counter.inc()


async def bound_fetch(session, pid, collection, counter, sem, output):

    async with sem:
        await fetch_article_meta_doi(session, pid, collection, counter, output)


async def main():
    parser = argparse.ArgumentParser(description="Utilitário para verificação de DOI")
    parser.add_argument(
        "file",
        help="Arquivo CSV contento PIDs e DOIs (opcional)",
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
    parser.add_argument(
        "--collection",
        help="Acrônimo da coleção a ser processada. Default: scl",
        type=str,
        default="scl",
    )

    args = parser.parse_args()

    PIDS = args.file.readlines()
    logger.info("Quantidade de linhas %s", len(PIDS))
    sem = asyncio.Semaphore(args.limit)
    tasks = []

    async with aiohttp.ClientSession() as session:
        counter = Counter()

        for pid in PIDS:
            tasks.append(
                bound_fetch(
                    session, pid.strip(), args.collection, counter, sem, args.output
                )
            )

        if tasks:
            logger.info("Quantidade de tasks registradas: %s", len(tasks))
            await asyncio.wait(tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(main())
