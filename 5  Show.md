# 🧠 PySpark DataFrame.show() — All-in-One Example + Summary
# ==========================================================
# This script demonstrates all the parameters of the show() method:
#    ➤ n          → number of rows to display
#    ➤ truncate   → controls column width (True = first 20 chars only)
#    ➤ vertical   → display data vertically
#
# Default: df.show(n=20, truncate=True, vertical=False)
# ------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ✅ Step 1: Create Spark Session
spark = SparkSession.builder.appName("ShowFunctionExample").getOrCreate()

# ✅ Step 2: Sample Data
data = [
    (1, 'goodjdngfkkkkkjjjjjjjjjjjjjjjjjjjjj'),
    (2, 'bad'),
    (3, 'ok'),
    (4, 'This is a very very long feedback text that exceeds twenty characters'),
    (5, 'nice')
]

# ✅ Step 3: Define Schema
schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('feedback', StringType(), True)
])

# ✅ Step 4: Create DataFrame
df = spark.createDataFrame(data=data, schema=schema)

# ✅ Step 5: Print Schema
print("\n📘 DataFrame Schema:")
df.printSchema()

# ✅ Step 6: Default show()
print("\n🔹 Default show() --> (n=20, truncate=True, vertical=False)")
df.show()

# ✅ Step 7: Show first 3 rows only
print("\n🔹 df.show(n=3) --> Displays first 3 rows only")
df.show(n=3)

# ✅ Step 8: Show full text without truncation
print("\n🔹 df.show(truncate=False) --> Shows full text for each column")
df.show(truncate=False)

# ✅ Step 9: Show vertically for better readability
print("\n🔹 df.show(vertical=True) --> Each row printed vertically")
df.show(vertical=True)

# ✅ Step 10: Combination of parameters
print("\n🔹 df.show(n=2, truncate=False, vertical=True) --> Combination example")
df.show(n=2, truncate=False, vertical=True)

# ✅ Step 11: (Optional) Show all rows - be careful for large datasets
print("\n⚠️ df.show(df.count(), truncate=False) --> Shows all rows (use cautiously)")
df.show(df.count(), truncate=False)

# ==========================================================
# 🧾 SUMMARY
# ==========================================================

print("""
==========================================================
🧩 PySpark show() — Parameter Summary
==========================================================

| Parameter | Default | Description |
|------------|----------|--------------|
| n | 20 | Number of rows to display |
| truncate | True | Cuts each string to 20 characters. Use False to see full text. |
| vertical | False | Shows each row vertically instead of side-by-side |

----------------------------------------------------------
🧠 Common Use Cases
----------------------------------------------------------
df.show()                          → Shows first 20 rows (truncated)
df.show(n=5)                       → Shows 5 rows
df.show(truncate=False)            → Shows full content (no truncation)
df.show(vertical=True)             → Prints records vertically
df.show(n=2, truncate=False, vertical=True) → Combination example
df.show(df.count(), truncate=False) → Shows all rows safely

----------------------------------------------------------
💡 Notes
----------------------------------------------------------
• Default behavior hides long text (only 20 chars visible).
• Use truncate=False when debugging text data.
• Use vertical=True for better readability with many columns.
• Be careful using df.count() inside show() — it loads the full dataset.

==========================================================
✅ End of Example
==========================================================
""")
