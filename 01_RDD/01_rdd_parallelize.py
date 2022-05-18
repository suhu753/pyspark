# coding:utf8

from pyspark import SparkConf,SparkContext

if __name__ == "__main__":
    # 通过conf对象实例化sparkcontext
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    # 通过并行化集合创建rdd, 本地集合转rdd
    rdd = sc.parallelize([1,2,3,4,5,6])
    # 默认分区数由电脑cpu核心所决定
    print("默认分区数：",rdd.getNumPartitions())

    # 自定义设置分区个数
    rdd_1 = sc.parallelize([1,2,3,4,5,6],3)
    print("此时的分区数为：",rdd_1.getNumPartitions())

    # rdd --> 本地集合
    print("输出是：",rdd.collect())