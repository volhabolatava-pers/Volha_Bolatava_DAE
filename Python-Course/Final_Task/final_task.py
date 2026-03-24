"""
Module for preparing inverted indexes based on uploaded documents
"""

## for review

import re
import json
import sys
from argparse import ArgumentParser, ArgumentTypeError, FileType
from io import TextIOWrapper
from typing import Dict, List
from collections import defaultdict

DEFAULT_PATH_TO_STORE_INVERTED_INDEX = "inverted.index"

STOP_WORDS = {
    "a", "an", "the", "and", "or", "in", "on", "at", "by", "is", "it", "of", "to", "for", "with"
}

class EncodedFileType(FileType):

    def __call__(self, string):
        # the special argument "-" means sys.std{in,out}
        if string == "-":
            if "r" in self._mode:
                stdin = TextIOWrapper(sys.stdin.buffer, encoding=self._encoding)
                return stdin
            if "w" in self._mode:
                stdout = TextIOWrapper(sys.stdout.buffer, encoding=self._encoding)
                return stdout
            msg = 'argument "-" with mode %r' % self._mode
            raise ValueError(msg)

        # all other arguments are used as file names
        try:
            return open(string, self._mode, self._bufsize, self._encoding, self._errors)
        except OSError as exception:
            args = {"filename": string, "error": exception}
            message = "can't open '%(filename)s': %(error)s"
            raise ArgumentTypeError(message % args)

    def print_encoder(self):
        """printer of encoder"""
        print(self._encoding)


class InvertedIndex:
    """
    This module is necessary to extract inverted indexes from documents.
    """

    def __init__(self, words_ids: Dict[str, List[int]]):
        self._index = words_ids

    def query(self, words: List[str]) -> List[int]:
        """Return the list of relevant documents for the given query"""
        
        if not words:
            return []
        
        words = [re.sub(r'[^\w\s]', '', w) for w in words]
        
        query_words = [w.lower() for w in words if w.lower() not in STOP_WORDS]

        if not query_words:
            return []
        
        result_sets = []

        for word in query_words:
            if word in self._index:
                result_sets.append(set(self._index[word]))
            else:
                return []

        result = set.intersection(*result_sets)
        return sorted(list(result))


    def dump(self, filepath: str) -> None:
        """
        Allow us to write inverted indexes documents to temporary directory or local storage
        :param filepath: path to file with documents
        :return: None
        """
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self._index, f)

    @classmethod
    def load(cls, filepath: str):
        """
        Allow us to upload inverted indexes from either temporary directory or local storage
        :param filepath: path to file with documents
        :return: InvertedIndex
        """
        try:    
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            data = {}      
        
        return cls(data)


def load_documents(filepath: str) -> Dict[int, str]:
    """
    Allow us to upload documents from either tempopary directory or local storage
    :param filepath: path to file with documents
    :return: Dict[int, str]
    """
    documents = {}
    
    try:
        f = open(filepath, "r", encoding="utf-8")
    except FileNotFoundError:
        f = open(filepath + ".txt", "r", encoding="utf-8")

    with f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            doc_id_str, text = line.split("\t", 1)
            documents[int(doc_id_str)] = text

    return documents

def build_inverted_index(documents: Dict[int, str]) -> InvertedIndex:
    """
    Builder of inverted indexes based on documents
    :param documents: dict with documents
    :return: InvertedIndex class
    """
    index = defaultdict(set)

    for doc_id, text in documents.items():
        clean_text = re.sub(r'[^\w\s]', '', text.lower())
        words = clean_text.split()

        unique_words = {w for w in words if w not in STOP_WORDS}

        for word in unique_words:
            index[word].add(doc_id)

    return InvertedIndex({word: sorted(ids) for word, ids in index.items()})


def callback_build(arguments) -> None:
    """process build runner"""
    return process_build(arguments.dataset, arguments.output)


def process_build(dataset, output) -> None:
    """
    Function is responsible for running of a pipeline to load documents,
    build and save inverted index.
    :param arguments: key/value pairs of arguments from 'build' subparser
    :return: None
    """
    documents: Dict[int, str] = load_documents(dataset)
    inverted_index = build_inverted_index(documents)
    inverted_index.dump(output)


def callback_query(arguments) -> None:
    """ "callback query runner"""
    process_query(arguments.query, arguments.index)


def process_query(queries, index) -> None:
    """
    Function is responsible for loading inverted indexes
    and printing document indexes for key words from arguments.query
    :param arguments: key/value pairs of arguments from 'query' subparser
    :return: None
    """
    inverted_index = InvertedIndex.load(index)

    if hasattr(queries, "read"):
        query_list = [line.strip().split() for line in queries if line.strip()]
    else:
        query_list = queries

    for words in query_list:
        if isinstance(words, str):
           words = words.split()
        result = inverted_index.query(words)
        print(",".join(map(str, result)))


def setup_subparsers(parser) -> None:
    """
    Initial subparsers with arguments.
    :param parser: Instance of ArgumentParser
    """
    subparser = parser.add_subparsers(dest="command")
    build_parser = subparser.add_parser(
        "build",
        help="this parser is need to load, build"
        " and save inverted index bases on documents",
    )
    build_parser.add_argument(
        "-d",
        "--dataset",
        required=True,
        help="You should specify path to file with documents. ",
    )
    build_parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_PATH_TO_STORE_INVERTED_INDEX,
        help="You should specify path to save inverted index. "
        "The default: %(default)s",
    )
    build_parser.set_defaults(callback=callback_build)

    query_parser = subparser.add_parser(
        "query", help="This parser is need to load and apply inverted index"
    )
    query_parser.add_argument(
        "--index",
        default=DEFAULT_PATH_TO_STORE_INVERTED_INDEX,
        help="specify the path where inverted indexes are. " "The default: %(default)s",
    )
    query_file_group = query_parser.add_mutually_exclusive_group(required=True)
    query_file_group.add_argument(
        "-q",
        "--query",
        dest="query",
        action="append",
        nargs="+",
        help="you can specify a sequence of queries to process them overall",
    )
    query_file_group.add_argument(
        "--query_from_file",
        dest="query",
        type=EncodedFileType("r", encoding="utf-8"),
        # default=TextIOWrapper(sys.stdin.buffer, encoding='utf-8'),
        help="query file to get queries for inverted index",
    )
    query_parser.set_defaults(callback=callback_query)


def main():
    """
    Starter of the pipeline
    """
    parser = ArgumentParser(
        description="Inverted Index CLI is need to load, build,"
        "process query inverted index"
    )
    setup_subparsers(parser)
    arguments = parser.parse_args()
    arguments.callback(arguments)


if __name__ == "__main__":
    main()
