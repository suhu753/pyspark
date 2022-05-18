# coding:utf8
import string
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
import pandas as pd
from pyspark.sql import functions as F


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse").\
        config("hive.metastore.uris", "thrift://node3:9083").\
        enableHiveSupport().\
        getOrCreate()
    sc = spark.sparkContext

    # 创建数据集
    rdd = sc.parallelize([
        ('张三', 'class_1', 99),
        ('王五', 'class_2', 95),
        ('王二', 'class_3', 97),
        ('王朝', 'class_4', 89),
        ('马汉', 'class_5', 96),
        ('吴飞', 'class_1', 98),
        ('马晓跳', 'class_3', 92),
        ('李逵', 'class_4', 91),
        ('张龙', 'class_5', 98)
    ])
    schema = StructType().add("name", StringType()).\
        add("class", StringType()).\
        add("score", IntegerType())
    df = rdd.toDF(schema=schema)

    # 注册临时表
    df.registerTempTable("stu")

    # TODO 1：聚合窗口函数
    spark.sql("""
        select *, avg(score) over() from stu
    """).show()

    # TODO 2: 排序窗口函数
    spark.sql("""
        SELECT *, ROW_NUMBER() OVER(ORDER BY score DESC) AS row_number_rank,
        DENSE_RANK() OVER(PARTITION BY class ORDER BY score DESC) AS densc_rank,
        RANK() OVER(ORDER BY score) AS rank
        from stu    
    """).show()

    # TODO 3: NTILE--均分函数，分成几份
    spark.sql("""
        select *, ntile(6) over(order by score desc) from stu 
    """).show()