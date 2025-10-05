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
