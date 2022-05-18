# coding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 从本地文件进行读取创建rdd
    file_rdd = sc.textFile("../data/input/words.txt")
    print("默认读取分区数：",file_rdd.getNumPartitions())
    print("file_rdd的内容：",file_rdd.collect())

    # 设置文件读取的分区数
    file_rdd1 = sc.textFile("../data/input/words.txt",3)
    # 最小分区数仅为参考，spark有自己的判断
    file_rdd2 = sc.textFile("../data/input/words.txt",100)
    print("file_rdd1的分区数：",file_rdd1.getNumPartitions())
    print("file_rdd2的分区数：",file_rdd2.getNumPartitions())

    # 从文件系统读取数据
    file_rdd3 = sc.textFile("hdfs://node1:8020/input/words.txt")
    print("file_rdd3的内容：",file_rdd3.collect())