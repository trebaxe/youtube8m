from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

appName = "PySpark - JSON naar Spark"
master = "local"

# Aanmaken Spark sessie
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# Schema aanmaken voor het Dataframe
schema = StructType([
    StructField('Title', StringType(), True),
    StructField('Videourl', StringType(), True),
    StructField('Category', StringType(), True),
    StructField('Description', StringType(), True)
])

# Aanmaken van Dataframe
    json_file_path = 'hdfs:///home/maria_dev/youtube8m/Youtube_Video_Dataset.json'
    df = spark.read.json(json_file_path, schema, multiLine=True)
    print(df.schema)
    df.show()