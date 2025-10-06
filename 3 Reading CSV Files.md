# Import necessary modules
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# 1️⃣ Basic Concept
# spark.read → 'read' is an attribute of the SparkSession object.
# It returns a DataFrameReader object, which is used to read data from various sources (like CSV, JSON, Parquet, etc.)

# -------------------------------------------------------------

# 2️⃣ Reading a Single CSV File (Method 1)
df1 = spark.read.csv(
    path='dbfs:/FileStore/tables/sample1.csv',
    header=True
)
display(df1)
df1.printSchema()

# -------------------------------------------------------------

# 3️⃣ Reading a Single CSV File (Method 2)
df2 = (
    spark.read
         .format('csv')
         .option('header', True)
         .load(path='dbfs:/FileStore/tables/sample2.csv')
)
display(df2)
df2.printSchema()

# -------------------------------------------------------------

# 4️⃣ Reading Multiple CSV Files with a Custom Schema
# You can pass a list of file paths and define a schema using StructType.

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

df3 = spark.read.csv(
    path=[
        'dbfs:/FileStore/tables/sample1.csv',
        'dbfs:/FileStore/tables/sample2.csv'
    ],
    schema=schema,
    header=True
)
display(df3)
df3.printSchema()



🧠 Summary

spark.read → Creates a DataFrameReader object.

.csv() → Reads data in CSV format.

.format('csv').option('header', True).load(path) → Another flexible way to read.

You can pass a single file path or a list of paths to read multiple files at once.

Use StructType and StructField to define a custom schema (recommended for better performance and control).
