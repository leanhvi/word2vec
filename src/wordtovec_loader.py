from __future__ import division  # py3 "true division"

import logging
import sys
import numpy as np
from numpy import dtype, fromstring
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from elasticsearch import Elasticsearch


if sys.version_info[0] >= 3:
    unicode = str

logger = logging.getLogger(__name__)


def to_unicode(text, encoding='utf8', errors='strict'):
    """Convert a string (bytestring in `encoding` or unicode), to unicode."""
    if isinstance(text, unicode):
        return text
    return unicode(text, encoding, errors=errors)


def load_word2vec_format(fname):
    """

    :param fname:
    :return:
    """
    logger.info("loading projection weights from %s", fname)
    with open(fname, 'rb') as fin:
        header = to_unicode(fin.readline())
        vocab_size, vector_size = (int(x) for x in header.split())
        print(" Vocabulary size: %s\nVector size: %s"%(vocab_size,vector_size))

        binary_len = dtype(np.float32).itemsize * vector_size
        for _ in range(vocab_size):
            # mixed text and binary: read text first, then binary
            word = []
            while True:
                ch = fin.read(1)
                if ch == b' ':
                    break
                if ch == b'':
                    raise EOFError("unexpected end of input; is count incorrect or file otherwise damaged?")
                if ch != b'\n':  # ignore newlines in front of words (some binary files have)
                    word.append(ch)
            word = to_unicode(b''.join(word))
            weights = fromstring(fin.read(binary_len), dtype=np.float32)

            # TODO Index to elasticsearch
            yield (word, weights)


def index_elasticsearch(fname, index, doc_type, start, stop, mute=False):
    """

    :param fname:
    :param index:
    :param doc_type:
    :param start:
    :param stop:
    :return:
    """
    x = load_word2vec_format(fname)
    es = Elasticsearch()

    print("START ID: %s\tSTOP ID:%s" % (start, stop))
    count = 0
    for i in x:
        if start is not None and count < start:
            count += 1
            continue
        if stop is not None and count >= stop:
            break
        word = i[0]
        vector = (i[1] / np.linalg.norm(i[1]))
        ordered_vector = vector + np.array(range(len(vector)))

        doc = {
            'id': count,
            'word': word,
            'vector': vector.tolist(),
            'ordered_vector': ordered_vector.tolist()
        }

        res = es.index(index=index, doc_type=doc_type, body=doc, id=count)
        if not mute:
            print("ID: %s\tWORD: %s" % (count, i[0]))

        count += 1

        if stop is not None and count > stop:
            break

def write_parquet(spark, srcFile, destFile, start, stop, mute=False):
    """

    :param srcFile:
    :param destFile:
    :param start:
    :param stop:
    :return:
    """
    x = load_word2vec_format(srcFile)

    print("START ID: %s\tSTOP ID:%s" % (start, stop))
    count = 0
    data = []
    for i in x:
        if start is not None and count < start:
            count += 1
            continue
        if stop is not None and count >= stop:
            break
        word = i[0]
        vector = (i[1] / np.linalg.norm(i[1])).tolist()
        data.append((count, word, vector))
        if not mute:
            print("ID: %s\tWORD: %s" % (count, i[0]))
        count += 1
        if stop is not None and count > stop:
            break

    df = spark.createDataFrame(data,["id", "word", "vector"])
    df.write.parquet(destFile, "append")


if __name__ == "__main__":

    vocab_size = 3000000

    srcFile = "/home/levi/word2vec/data/GoogleNews-vectors-negative300.bin"
    destFile = "/home/levi/word2vec/data/wordtovec_google.parquet"

    sc = SparkContext()
    spark = SparkSession(sc)

    for i in range(0, vocab_size, 50000):
        start = i
        stop = i + 50000
        stop = min(stop, vocab_size)
        write_parquet(spark=spark, srcFile=srcFile, destFile=destFile, start=start, stop=stop, mute=True)
        print("SUCCESS FROM %s TO %s" % (start, stop))


