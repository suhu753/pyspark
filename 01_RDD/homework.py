# coding: utf8

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel


if __name__ == '__main__':
    # 0. 初始化执行环境 构建SparkContext对象
    conf = SparkConf().setAppName("home_work").setMaster("local[*]")
    # conf = SparkConf().setAppName("test-on-yarn").setMaster("yarn")
    sc = SparkContext(conf=conf)

    # 读取文件
    file_rdd = sc.textFile("../data/input/apache.log")

    # 2. 对数据进行切分 \t
    split_rdd = file_rdd.map(lambda x: x.split(" "))

    # 3. 因为要做多个需求, split_rdd 作为基础的rdd 会被多次使用.
    split_rdd.persist(StorageLevel.DISK_ONLY)  # 仅持久化保存在磁盘中
    # split_rdd.cache()  # 持久化到内存中
    # 查看前3个个数据
    # print(split_rdd.takeSample(True, 3, 123))

    # TODO：需求1：计算当前网站的PV(被访问量)
    print("总PV为：", split_rdd.count())

    # TODO：需求2： 当前访问的UV(访问的用户数)
    # 注意：这里需要对用户id进行去重操作
    # 取出用户id
    user_rdd = split_rdd.map(lambda x: x[1])
    # 对其进行去重操作
    print("总的 UV 为：", user_rdd.distinct(numPartitions=1).count())
    # print(user_rdd.collect())

    # TODO：需求3：有哪些 IP 访问了网站
    # 首先获取ip
    ip_rdd = split_rdd.map(lambda x: x[0])
    # 去重获取不同的ip
    print("访问的ip有：", ip_rdd.distinct().collect())

    # TODO：需求4：哪个页面的访问量最高
    # 获取页面url,并对其进行分组(url,1), 聚合，排序 取第一
    url_rdd = split_rdd.map(lambda x: (x[4], 1)).reduceByKey(lambda a, b: a+b).\
        sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(1)[0][0]
    print("访问量最高的页面url为：", url_rdd)

    split_rdd.unpersist()  # 释放缓存