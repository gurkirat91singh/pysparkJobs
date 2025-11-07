from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PiExample").getOrCreate()
num_samples = 10000

def inside(p):
    import random
    x, y = random.random(), random.random()
    return x*x + y*y < 1

count = spark.sparkContext.parallelize(range(0, num_samples)).filter(inside).count()
print(f"Pi is roughly {4.0 * count / num_samples}")

spark.stop()
