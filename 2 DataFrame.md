# 🧠 PySpark DataFrame — Complete Overview

## 📘 What is a DataFrame?

A **DataFrame** is a **distributed collection of data** organized into **named columns**.  
It is conceptually equivalent to a **table in a relational database**.

---

## 🔹 Key Concepts

| Concept | Description | Example |
|----------|--------------|----------|
| **Distributed** | Data is split across multiple cluster nodes. | Large CSV file split across 4 machines |
| **Named Columns** | Like headers in a table. | `emp_id`, `emp_name`, `salary` |
| **Equivalent to SQL Table** | You can use SQL syntax for querying. | `SELECT * FROM employee` |

---

## 🔹 Creating a DataFrame

A DataFrame is created using the **`createDataFrame()`** method from the **SparkSession** object.  
By default, Spark automatically infers the data types of each column.

```python
# DataFrame Creation Example
lst = [(1.0, 'mohan'), (2.0, 'raj')]
sch = ['id', 'name']

df = spark.createDataFrame(data=lst, schema=sch)
df.show()
df.printSchema()


+---+-----+
| id| name|
+---+-----+
|1.0|mohan|
|2.0|  raj|
+---+-----+

root
 |-- id: double (nullable = true)
 |-- name: string (nullable = true)


🔹 Defining Schema Manually

If you want to explicitly specify data types instead of letting Spark infer them,
use StructType() and StructField() from pyspark.sql.types.


from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# Sample data
lst = [(1, 'mohan'), (2, 'raj')]

# Define schema manually
schema = StructType([
    StructField(name='id', dataType=IntegerType()),
    StructField(name='name', dataType=StringType())
])

# Create DataFrame with defined schema
df = spark.createDataFrame(data=lst, schema=schema)
df.show()
df.printSchema()


✅ Output
+---+-----+
| id| name|
+---+-----+
|  1|mohan|
|  2|  raj|
+---+-----+

root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)


🔹 Summary

A DataFrame is like a distributed SQL table.

It supports both automatic and manual schema definitions.

You can perform operations using either:

The DataFrame API, or

SQL queries via spark.sql().
