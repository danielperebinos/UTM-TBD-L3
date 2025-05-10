import logging

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Starting Spark Testing Session")
spark = SparkSession.builder \
    .appName("PySpark Docker Example") \
    .getOrCreate()

logger.info("Creating DataFrame")
data = [("Alice", 30), ("Bob", 25), ("Alice", 35), ("Charlie", 40)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

logger.info("Performing transformations")
result = df.filter(df.age > 30).groupBy("name").count()

logger.info("Displaying results")
result.show()

logger.info("Stopping Spark session")
spark.stop()
