### ✅ RDD (Resilient Distributed Dataset) – Core building block of PySpark

- **Immutable**, **Fault-tolerant**, **Distributed** collection of objects
- Data is **logically partitioned** → computations run **in parallel across cluster nodes**
- Similar to a **Python list**, but:
  - **List = stored on one machine**
  - **RDD = distributed across multiple machines**
- Provides **abstraction of partitioning & distribution** → you **don’t manage parallelism manually**
- Each RDD has a **name** and a **unique ID**

---

### ⭐ Key Benefits of RDD

#### ✅ In-Memory Processing
Loads data from disk → processes & keeps it **in-memory**  
➜ Faster than MapReduce (which is I/O intensive)

#### ✅ Immutability
Once created, RDD **cannot be modified**  
➜ Transformations create **new RDDs**  
➜ **RDD Lineage** is maintained

#### ✅ Fault Tolerance
Uses reliable storage (HDFS, S3, etc.)  
➜ If a partition fails, Spark **recomputes from lineage**  
➜ Task failures are **auto-retried**

#### ✅ Lazy Evaluation
Transformations are **not executed immediately**  
➜ Stored as **DAG (Directed Acyclic Graph)**  
➜ Execution happens **only when an action is called**

#### ✅ Automatic Partitioning
RDD is **automatically split** based on **available cores**  
➜ Enables **parallel processing** effortlessly
