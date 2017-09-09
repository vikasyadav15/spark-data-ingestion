"word count usin python spark"

# pylint: disable-msg=E0401
from operator import add
from pyspark import SparkConf, SparkContext


APP_NAME = "WORD COUNT - PYSPARK"
FILE_PATH = "/home/vitwit/Documents/Datasets/textfile.txt"

# pylint: disable-msg=W0621
def main(sc, filename):
    "This fuction computes word count"
    # pylint: disable-msg=C0103
    textRDD = sc.textFile(filename)
    words = textRDD.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))
    wordcount = words.reduceByKey(add).collect()
    for wcount in wordcount:
        print wcount[0], wcount[1]

if __name__ == "__main__":
    # Configure Spar
    # pylint: disable-msg=C0103
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # Execute Main functionality
    main(sc, FILE_PATH)
