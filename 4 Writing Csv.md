# PySpark: Writing and Reading CSV Example

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# 1️⃣ Create SparkSession
spark = SparkSession.builder.appName("CSVExample").getOrCreate()

# 2️⃣ Define schema
schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('salary', IntegerType(), True)
])

# 3️⃣ Create sample DataFrame
df = spark.createDataFrame(
    [(1, 'Moule', 1000),
     (2, 'Aar', 4000),
     (3, 'Raj', 6000),
     (4, 'Mohan', 6000)],
    schema=schema
)

# Display DataFrame
df.show()

# 4️⃣ Write DataFrame to CSV
df.write.csv(
    path='dbfs:/tmp/emp',   # Target path
    mode='overwrite',       # Overwrite existing files
    header=True             # Include column names
)

# Explanation:
# write -> returns DataFrameWriter object
# csv() -> saves data in CSV format
# mode options:
#   append    -> add to existing data
#   overwrite -> replace existing data
#   error     -> default, throws error if path exists
#   ignore    -> skip writing if path exists

# 5️⃣ Read the CSV file back
df_read = spark.read.csv(
    'dbfs:/tmp/emp',
    header=True,      # Read column names
    inferSchema=True  # Infer data types automatically
)

# Display the read DataFrame
df_read.show()
df_read.printSchema()
