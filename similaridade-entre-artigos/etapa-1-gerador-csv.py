import os
import re
import asyncio
import argparse
import logging
from logging import config

import aiohttp
import lxml
from bs4 import BeautifulSoup
from unidecode import unidecode

logging.basicConfig(format=u"%(asctime)s %(levelname)-5.5s [%(name)s] %(message)s")

logger = logging.getLogger(__name__)


SITE_INSTANCES = [
    {
        "url": "http://www.scielo.br/scielo.php?script=sci_arttext&pid={}",
        # Alterei a forma de seleção do elemento root
        # "query_root": {"class": "content"},
        "query_root": {"class": re.compile("^index.*")},
        "remove": [
            {"id": "group", "container": "div"},
            {"class_": "footer", "container": "div"},
            {"class_": "license", "container": "div"},
            {"class_": "copyright", "container": "div"},
            {"href": "javascript:void(0);", "container": "a"},
        ],
        "name": "classic",
        # Adicionei mais elementos para que o script busque o texto
        "text_nodes": [],
    },
    {
        "url": "http://new.scielo.br/article/{}",
        # Alterei a forma de seleção do elemento root
        "query_root": {"id": "standalonearticle"},
        "remove": [
            {"class_": "articleMenu", "container": "div"},
            {"class_": "floatingMenuCtt", "container": "div"},
            {"class_": "articleTimeline", "container": "ul"},
            # {"class_": "articleBadge-editionMeta-doi-copyLink", "container": "div"},
            {"class_": "documentLicense", "container": "section"},
            # {"class_": "fig", "container": "div"}, # está removendo figuras lol
            {"class_": "thumb", "container": "div"},
            {"class_": "outlineFadeLink", "container": "a"},
            # Adicionei
            {"container": "script"},
            {"class_": "refCtt", "container": "span"},
            {"class_": "big", "container": "sup"},
            {"data-anchor": "Datas de Publicação ", "container": "div"},
            {"data-anchor": "Publication Dates", "container": "div"},
            {"container": "button"},
            {"class_": "thumb", "container": "div"},
            # {"class_": "ref footnote", "container": "span"},
            {"id": "ModalArticles", "container": "div"},
            {"container": "a", "data-target": "#ModalTutors"},
            {"container": "div", "class_": "modal-header"},
            {"container": "div", "id": "ModalDownloads"},
            {"container": "div", "id": "ModalRelatedArticles"},
            {"container": "div", "id": "ModalVersionsTranslations"},
            {"container": "div", "class_": "floatingMenuMobile"},
            {"container": "div", "id": "metric_modal_id"},
            {"container": "div", "id": "share_modal_id"},
            {"container": "div", "id": "share_modal_confirm_id"},
            {"container": "div", "id": "error_modal_id"},
            {"container": "div", "id": "error_modal_confirm_id"},
            {"container": "a", "class_": "copyLink"},
            {"container": "span", "class_": "_separator"},
            {"container": "span", "class_": "_editionMeta"},
            {"container": "span", "class_": "_separator"},
            {"container": "span", "class_": "_articleBadge"},
            {"container": "div", "id": "ModalTablesFigures"},
        ],
        "name": "new",
        # Adicionei mais elementos para que o script busque o texto
        "text_nodes": [],
    },
]


class Counter:
    def __init__(self, count=0):
        self.count = count

    def inc(self):
        self.count += 1

        if self.count % 100 == 0:
            logger.info("Quantidade de artigos comparados: %s.", self.count)


def get_html_text(soup, query_root, text_nodes, node="div"):

    result = []
    root = soup.find(node, query_root)

    if root is None:
        return result

    # strings = [string for string in root.strings]

    for string in root.strings:
        string = string.strip()
        string = re.sub(r"\s+", " ", string)
        string.replace("[ Links ]", "")

        if len(string) > 0:
            result.append(string)

    return result


def remove_elements(soup, container, **options):
    for element in soup.find_all(container, **options):
        element.extract()


def normalize(text):
    TEXT_REGEX = re.compile(r"[^\w\s]")
    text = TEXT_REGEX.sub(" ", unidecode(text).lower())
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def get_text_statistics(query, document, normalizefunc=normalize):
    query = normalize(query)
    document = normalize(document)

    query_split = [q for q in query.split(" ") if len(q) > 3]
    document_split = [d for d in document.split(" ") if len(d) > 3]

    query_word_len = len(query_split)
    document_word_len = len(document_split)
    intersection = set(query_split).intersection(set(document_split))
    union = set(query_split).union(set(document_split))
    union_minus_intersection = union - intersection

    return locals()


def jaccard_similarity(query, document):
    query = normalize(query)
    document = normalize(document)

    query_split = [q for q in query.split(" ") if len(q) > 3]
    document_split = [d for d in document.split(" ") if len(d) > 3]

    intersection = set(query_split).intersection(set(document_split))
    union = set(query_split).union(set(document_split))
    return len(intersection) / float(len(union))


async def fetch_article(session, pid, counter):
    extracted_texts = {}
    articles_paragraphs = []

    try:
        for instance in SITE_INSTANCES:
            async with session.get(instance["url"].format(pid)) as response:
                if response.status != 200:
                    logger.error(
                        "Article not found '%s' in instance '%s'",
                        pid,
                        instance["url"].format(pid),
                    )
                    return

                content = await response.content.read()
                html = content.decode("utf-8")
                soup = BeautifulSoup(html, "lxml")
                # soup = BeautifulSoup(html, "html.parser")

                for element_to_remove in instance["remove"]:
                    remove_elements(soup, **element_to_remove)

                # [remove_elements(soup, **el) for el in instance["remove"]]

                paragraphs = get_html_text(
                    soup,
                    query_root=instance["query_root"],
                    text_nodes=instance["text_nodes"],
                )

                if instance.get("name") == "classic":
                    ps = get_html_text(
                        BeautifulSoup(html, "lxml"),
                        query_root={"id": "doi"},
                        text_nodes=[],
                        node="h4",
                    )

                    for node, query in [
                        ["h4", {"id": "doi"}],
                        ["p", {"class": "categoria"}],
                    ]:

                        ps = get_html_text(
                            BeautifulSoup(html, "lxml"),
                            query_root=query,
                            text_nodes=[],
                            node=node,
                        )
                        paragraphs.extend(ps)
                articles_paragraphs.append(paragraphs)
                extracted_texts[instance["name"]] = " ".join(paragraphs).lower()

        # query == site clássico, document == new
        similarity = jaccard_similarity(
            query=extracted_texts["classic"], document=extracted_texts["new"]
        )
        statistics = get_text_statistics(
            query=extracted_texts["classic"], document=extracted_texts["new"]
        )

        saida = "{};{:.4f};{};{};{};{};{}".format(
            pid,
            similarity,
            statistics["query_word_len"],
            statistics["document_word_len"],
            len(statistics["intersection"]),
            len(statistics["union"]),
            statistics["union"] - statistics["intersection"],
        )

        print(saida)
        with open("output.csv", "a") as f:
            f.write(saida + "\n")
    except Exception as err:
        logger.error("Exception for %s.", pid)
        logger.exception(err)


async def bound_fetch(fetcher, session, pid, counter, sem):

    async with sem:
        await fetcher(session, pid, counter)


async def main(file: str):
    sem = asyncio.Semaphore(20)
    tasks = []

    with open(file) as f:
        PIDS = f.readlines()

    async with aiohttp.ClientSession() as session:
        counter = Counter()

        for PID in PIDS:
            tasks.append(bound_fetch(fetch_article, session, PID.strip(), counter, sem))

        if tasks:
            logger.info("Quantidade de tasks registradas: %s", len(tasks))
            responses = asyncio.gather(*tasks)
            await responses


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    parser = argparse.ArgumentParser()
    parser.add_argument("file")

    args = parser.parse_args()

    loop.run_until_complete(main(args.file))
