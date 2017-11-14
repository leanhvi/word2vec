from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from src.wordtovec import DataLoader
from pyspark.ml.feature import Word2Vec, Word2VecModel

if __name__ == "__main__":
    sc = SparkContext()
    spark = SparkSession(sparkContext=sc)
    # loader = DataLoader()
    # dir = "/home/levi/data/American_literature/"
    # df = loader.collectData(spark, dir)
    # total = df.count()
    # print("TOTAL SENTENCE: %s" % total)
    #
    # dataPath = "/home/levi/projects/wordtovec/data/df-%s.parquet" % total
    # df.write.parquet(dataPath)

    df = spark.read.load("/home/levi/projects/wordtovec/data/df-1365637.parquet")

    w2v = Word2Vec(vectorSize=100, maxIter=100, inputCol="sentences", outputCol="vectors")

    model = w2v.fit(dataset=df)
    #
    modelPath = "/home/levi/projects/wordtovec/data/model_4"
    model.save(modelPath)

    # trainingTextPath = "/home/levi/projects/wordtovec/data/trainingset.txt"
    # loader.buildData(dir, trainingTextPath)
