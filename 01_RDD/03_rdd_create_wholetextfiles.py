# coding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    rdd = sc.wholeTextFiles("../data/input/tiny_files")
    print(rdd.collect())
    print(rdd.map(lambda x:x[1]).collect())