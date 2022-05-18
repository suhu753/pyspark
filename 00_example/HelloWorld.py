# coding:utf8
# import findspark
# findspark.init()

from pyspark import SparkConf, SparkContext
# import os
# os.environ['PYSPARK_PYTHON'] = "D:\\Anaconda3\\envs\\pyspark\\python\\python.exe"

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("WordCountHelloWorld")
    # conf = SparkConf().setAppName("WordCountHelloWorld")
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # 需求：wordcount单词计数
    # 读取文件
    # file_rdd = sc.textFile("../data/input/words.txt")
    file_rdd = sc.textFile("hdfs://node1:8020/input/words.txt")

    # 将单词进行切割，得到一个存储全部单词的集合对象
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))

    # 将单词转换为元组对象,key为单词，value是数字
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))

    # 将元组的value 按照key来分组，对所有的value执行据合操作（相加）
    result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # 通过collect方法收集RDD的数据打印输出结果
    print(result_rdd.collect())