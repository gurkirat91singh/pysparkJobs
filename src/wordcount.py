from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()
text_file = spark.sparkContext.parallelize(["hello world hello spark"])
counts = text_file.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)
print(counts.collect())
spark.stop()
