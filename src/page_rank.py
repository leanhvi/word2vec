from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

def process_line(line):
    lst = line.split(" ")
    v = lst[0]
    nb = []
    if len(lst) > 0:
        nb = lst[1:]
    return (v, nb)

def load_map(mapfile):
    rdd = sc.textFile(mapfile)
    graph = rdd.map(process_line)
    return graph





