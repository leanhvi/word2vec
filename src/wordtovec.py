import codecs, os, re
from pyspark.ml.feature import Word2Vec, Word2VecModel

class DataLoader:

    def tokenize(self, sentence):
        """

        :param sentence:
        :return:
        """
        regex = re.compile('[^a-zA-Z]')
        words = sentence.split()
        cleanWords = [regex.sub('', word) for word in words]
        return cleanWords

    def readTextFile(self, filePath):
        """

        :param filePath:
        :return:
        """
        with codecs.open(filePath, 'r', encoding="ISO-8859-1", errors='ignore') as file:
            text = file.read()
        return text

    def createDataFrame(self, spark, filePath):
        """

        :param spark:
        :param filePath:
        :return:
        """
        print("START READING DATA: %s" % filePath)
        text = self.readTextFile(filePath)
        lines = text.lower().split(". ")
        sentences = [(self.tokenize(line),) for line in lines]
        df_sentences = spark.createDataFrame(sentences, ["sentences"])
        return df_sentences

    def collectData(self, spark, dir):
        """

        :param spark:
        :param dir:
        :return:
        """
        files = self.unix_find(dir, ".txt")
        lst_df = []
        for file in files:
            df_iter = self.createDataFrame(spark, file)
            lst_df.append(df_iter)

        df = lst_df[0]
        for i in range(1,len(lst_df)):
            df = df.union(lst_df[i])

        return df

    def unix_find(self, pathin, suffix):
        """Return results similar to the Unix find command run without options
        i.e. traverse a directory tree and return all the file paths
        """
        files = [os.path.join(path, file)
                for (path, dirs, files) in os.walk(pathin)
                for file in files if file.endswith(suffix)]

        print("TOTAL FILES: %s" % len(files))
        return files


    def buildData(self, dir, outFile):
        """

        :param dir:
        :param outFile:
        :return:
        """
        files = self.unix_find(dir, ".txt")
        writer = open(file=outFile, mode="w")

        for file in files:
            print("START READING DATA: %s" % file)
            text = self.readTextFile(file)
            text = text.lower()
            sents = text.split(". ")
            txt = ""
            for sent in sents:
                tokens = self.tokenize(sent)
                for word in tokens:
                    txt += " " + word
                txt += "\n\n"

            writer.write(txt)

        writer.close()



