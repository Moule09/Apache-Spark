# PySpark - withColumn() Example
# --------------------------------
# -> withColumn() is a method of DataFrame class
# -> Used to add a new column or modify an existing column
# -> Syntax: withColumn(colName, expr)
# -> expr must be a Column object (not a plain string)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Create Spark session
spark = SparkSession.builder.appName("withColumn Example").getOrCreate()

# Sample data
data = [
    (1, "Monkey", 3000),
    (2, "Hari", 4000),
    (3, "Moule", 5000),
    (4, "Aar", 8990)
]
columns = ["emp_id", "emp_name", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()

# Add a new column 'age' using lit() function
# lit() is a function that returns a Column object with a literal value
df1 = df.withColumn("age", lit(20))
df1.show()

# Modify existing column datatype using cast()
# col() returns a Column class object
# cast() is a method of the Column class
df2 = df.withColumn("salary", col("salary").cast("integer"))
df2.printSchema()
