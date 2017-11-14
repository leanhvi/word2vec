from gensim.models.keyedvectors import KeyedVectors

wordvector = KeyedVectors.load_word2vec_format(fname='/home/levi/word2vec/data/am_vectors.bin', binary=True)


import struct




# struct.unpack('c', b[0:4])