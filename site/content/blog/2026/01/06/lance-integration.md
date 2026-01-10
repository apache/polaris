---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: "Apache Polaris and Lance: Bringing AI-Native Storage to the Open Multimodal Lakehouse"
date: 2026-01-06
author: Jack Ye and Yun Zou
---

## Introduction

We are excited to announce the integration between Apache Polaris and the Lance ecosystem,
enabling users to manage Lance tables through the Apache Polaris Generic Table API.
This integration brings AI-native columnar storage to the open multimodal lakehouse,
allowing organizations to leverage Apache Polaris as a unified catalog for both Iceberg and Lance tables.

## What is Lance?

[Lance](https://lance.org) is an open lakehouse format designed for multimodal AI workloads.
It contains a file format, table format, and catalog spec that allows you to build a complete
multimodal lakehouse on top of object storage to power your AI workflows.
The key features of Lance include:

- **Expressive hybrid search:** Combine vector similarity search, full-text search (BM25),
  and SQL analytics on the same dataset with accelerated secondary indices.

- **Lightning-fast random access:** 100x faster than Parquet or Iceberg for random access
  without sacrificing scan performance.

- **Native multimodal data support:** Store images, videos, audio, text, and embeddings
  in a single unified format with efficient blob encoding and lazy loading.

- **Data evolution:** Efficiently add columns with backfilled values without full table rewrites,
  perfect for ML feature engineering.

- **Zero-copy versioning:** ACID transactions, time travel, and automatic versioning
  without needing extra infrastructure.

- **Rich ecosystem integrations:** Apache Arrow, Pandas, Polars, DuckDB, Apache Spark, Ray, Trino,
  Apache Flink, and open catalogs (Apache Polaris, Unity Catalog, Apache Gravitino).

### What is Lance Namespace?

[**Lance Namespace**](https://lance.org/format/namespace) is the catalog spec layer for the Lance Open Lakehouse Format.
While Lance tables can be stored directly on object storage, production AI/ML workflows
require integration with enterprise metadata services for governance, access control, and discovery.

Lance Namespace addresses this need by defining both native catalog spec as well as
a standardized framework for accessing and operating on a collection of Lance tables 
across different open catalog specs including Apache Polaris.

Here are some example systems and how they are mapped in Lance Namespace:

| System         | Structure                                      | Lance Namespace Mapping                     |
|----------------|------------------------------------------------|---------------------------------------------|
| Directory      | `/data/users.lance`                            | Table `["users"]`                           |
| Hive Metastore | `database.table`                               | Table `["default", "orders"]`               |
| Apache Polaris | `/my-catalog/namespaces/team_a/tables/vectors` | Table `["my-catalog", "team_a", "vectors"]` |

Apache Polaris supports arbitrary namespace nesting, 
making it particularly flexible for organizing Lance tables in complex data architectures.

## What is the Generic Table API in Apache Polaris?

Apache Polaris is best known as an open-source catalog for Apache Iceberg.
In addition, Apache Polaris also offers the [**Generic Table API**]({{% ref "../../../../in-dev/unreleased/generic-table.md" %}})
that can be used for managing non-Iceberg table formats such as Delta, Apache Hudi, Lance, and others.

### Generic Table Definition

A generic table in Apache Polaris is an entity with the following fields:

| Field             | Required | Description                                                           |
|-------------------|----------|-----------------------------------------------------------------------|
| **name**          | Yes      | Unique identifier for the table within a namespace                    |
| **format**        | Yes      | The table format (e.g., `delta`, `csv`, `lance`)                      |
| **base-location** | No       | Table base location in URI format (e.g., `s3://bucket/path/to/table`) |
| **properties**    | No       | Key-value properties for the table                                    |
| **doc**           | No       | Comment or description for the table                                  |

Generic tables share the same namespace hierarchy as Iceberg tables, 
and table names must be unique within a namespace regardless of format.

### Generic Table API vs. Iceberg Table API

Apache Polaris provides separate API endpoints for generic tables and Iceberg tables:

| Operation    | Iceberg Table API Endpoint                         | Generic Table API Endpoint                                 |
|--------------|----------------------------------------------------|------------------------------------------------------------|
| Create Table | `POST .../namespaces/{namespace}/tables`           | `POST .../namespaces/{namespace}/generic-tables`           |
| Load Table   | `GET .../namespaces/{namespace}/tables/{table}`    | `GET .../namespaces/{namespace}/generic-tables/{table}`    |
| Drop Table   | `DELETE .../namespaces/{namespace}/tables/{table}` | `DELETE .../namespaces/{namespace}/generic-tables/{table}` |
| List Tables  | `GET .../namespaces/{namespace}/tables`            | `GET .../namespaces/{namespace}/generic-tables`            |

The Iceberg Table APIs handle the management of Iceberg tables, while the Generic Table APIs manage Generic (non-Iceberg) tables.
This clear separation enforces well-defined boundaries between table formats, while still allowing them to coexist within the same catalog and namespace structure.

## Lance Integration with Generic Table API

The Lance Namespace implementation for Apache Polaris maps Lance Namespace operations to the Generic Table API. 
Lance tables are registered as generic tables with the `format` field set to `lance`, 
and the `base-location` pointing to the Lance table root directory.

### Table Identification

A table in Apache Polaris is identified as a Lance table when:
- It is registered as a Generic Table
- The `format` field is set to `lance`
- The `base-location` points to a valid Lance table root directory
- The `properties` contain `table_type=lance` for consistency

### Supported Operations

The Lance Namespace Apache Polaris implementation supports the following operations:

| Operation         | Description                                             |
|-------------------|---------------------------------------------------------|
| CreateNamespace   | Create a new namespace hierarchy                        |
| ListNamespaces    | List child namespaces                                   |
| DescribeNamespace | Get namespace properties                                |
| DropNamespace     | Remove a namespace                                      |
| DeclareTable      | Declare a new table exists at a given location          |
| ListTables        | List all Lance tables in a namespace                    |
| DescribeTable     | Get table metadata and location                         |
| DeregisterTable   | Deregister a table from the namespace (no data removal) |

## Using Lance with Apache Polaris

The power of the Lance and Apache Polaris integration is that you can now store Lance tables in Apache Polaris
and access them from any engine that supports Lance.
Whether you're ingesting data with Spark, running feature engineering with Ray,
building RAG applications with LanceDB, or analyzing with Trino,
all these engines can work with the same Lance tables managed through Apache Polaris.

Let's walk through a complete end-to-end workflow using the [BeIR/quora](https://huggingface.co/datasets/BeIR/quora)
dataset from Hugging Face to build a question-answering system.

### Step 1: Ingest Data with Apache Spark

First, use Spark to load the Quora dataset and write it to a Lance table in Apache Polaris:

```python
from pyspark.sql import SparkSession
from datasets import load_dataset

# Create Spark session with Apache Polaris catalog
spark = SparkSession.builder \
    .appName("lance-polaris-ingest") \
    .config("spark.jars.packages", "org.lance:lance-spark-bundle-3.5_2.12:0.0.7") \
    .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog") \
    .config("spark.sql.catalog.lance.impl", "polaris") \
    .config("spark.sql.catalog.lance.endpoint", "http://localhost:8181") \
    .config("spark.sql.catalog.lance.auth_token", "<your-token>") \
    .getOrCreate()

# Create namespace for ML workloads
spark.sql("CREATE NAMESPACE IF NOT EXISTS lance.my_catalog.ml")

# Load Quora dataset from Hugging Face
dataset = load_dataset("BeIR/quora", "corpus", split="corpus[:10000]", trust_remote_code=True)
pdf = dataset.to_pandas()
pdf = pdf.rename(columns={"_id": "id"})

# Convert to Spark DataFrame and write to Lance table
df = spark.createDataFrame(pdf)
df.writeTo("lance.my_catalog.ml.quora_questions").create()

# Verify the data
spark.sql("SELECT COUNT(*) FROM lance.my_catalog.ml.quora_questions").show()
```

### Step 2: Feature Engineering with Ray

Next, use Ray's distributed computing to generate embeddings for all documents:

```python
import ray
import pyarrow as pa
import lance_namespace as ln
from lance_ray import add_columns

ray.init()

# Connect to Apache Polaris
namespace = ln.connect("polaris", {
    "endpoint": "http://localhost:8181",
    "auth_token": "<your-token>"
})

def generate_embeddings(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Generate embeddings using sentence-transformers."""
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer('BAAI/bge-small-en-v1.5')

    texts = []
    for i in range(len(batch)):
        title = batch["title"][i].as_py() or ""
        text = batch["text"][i].as_py() or ""
        texts.append(f"{title}. {text}".strip())

    embeddings = model.encode(texts, normalize_embeddings=True)

    return pa.RecordBatch.from_arrays(
        [pa.array(embeddings.tolist(), type=pa.list_(pa.float32(), 384))],
        names=["vector"]
    )

# Add embeddings column using distributed processing
add_columns(
    uri=None,
    namespace=namespace,
    table_id=["my_catalog", "ml", "quora_questions"],
    transform=generate_embeddings,
    read_columns=["title", "text"],
    batch_size=100,
    concurrency=4,
)

print("Embeddings generated successfully!")
```

### Step 3: SQL Analytics with Trino

Use Trino for SQL analytics on the same dataset:

```properties
# etc/catalog/lance.properties
connector.name=lance
lance.impl=polaris
lance.endpoint=http://localhost:8181
lance.auth_token=<your-token>
```

```sql
-- Explore the dataset
SHOW SCHEMAS FROM lance;
SHOW TABLES FROM lance.my_catalog.ml;
DESCRIBE lance.my_catalog.ml.quora_questions;

-- Basic analytics
SELECT COUNT(*) as total_questions
FROM lance.my_catalog.ml.quora_questions;

-- Find questions by keyword
SELECT id, title, text
FROM lance.my_catalog.ml.quora_questions
WHERE text LIKE '%machine learning%'
LIMIT 10;

-- Aggregate statistics
SELECT
    LENGTH(text) as text_length,
    COUNT(*) as count
FROM lance.my_catalog.ml.quora_questions
GROUP BY LENGTH(text)
ORDER BY count DESC
LIMIT 10;
```

### Step 4: Agentic Search with LanceDB

Finally, use LanceDB for AI-native semantic search and full-text search on the enriched dataset:

```python
import lancedb
from sentence_transformers import SentenceTransformer

# Connect to Apache Polaris via LanceDB
db = lancedb.connect_namespace(
    "polaris",
    {
        "endpoint": "http://localhost:8181",
        "auth_token": "<your-token>"
    }
)

# Open the table with embeddings
table = db.open_table("quora_questions", namespace=["my_catalog", "ml"])

# Create vector index for fast similarity search
table.create_index(
    metric="cosine",
    vector_column_name="vector",
    index_type="IVF_PQ",
    num_partitions=32,
    num_sub_vectors=48,
)

# Create full-text search index
table.create_fts_index("text")
```

#### Vector Search

```python
model = SentenceTransformer('BAAI/bge-small-en-v1.5')
query_text = "How do I learn machine learning?"
query_embedding = model.encode([query_text], normalize_embeddings=True)[0]

results = (
    table.search(query_embedding, vector_column_name="vector")
    .limit(5)
    .to_pandas()
)

print("=== Vector Search Results ===")
for idx, row in results.iterrows():
    print(f"{idx + 1}. {row['title']}")
    print(f"   {row['text'][:150]}...")
```

#### Full-Text Search

```python
results = (
    table.search("machine learning algorithms", query_type="fts")
    .limit(5)
    .to_pandas()
)

print("=== Full-Text Search Results ===")
for idx, row in results.iterrows():
    print(f"{idx + 1}. {row['title']}")
    print(f"   {row['text'][:150]}...")
```

#### Hybrid Search

```python
results = (
    table.search(query_embedding, vector_column_name="vector")
    .where("text LIKE '%python%'", prefilter=True)
    .limit(5)
    .to_pandas()
)
```

## Try It Yourself

The [lance-namespace-impls](https://github.com/lance-format/lance-namespace-impls) repository provides a 
[Docker Compose setup](https://github.com/lance-format/lance-namespace-impls/tree/main/docker/polaris) 
that makes it easy to try out the Apache Polaris integration locally.

### Quick Start

```bash
git clone https://github.com/lance-format/lance-namespace-impls.git
cd lance-namespace-impls/docker

# Start Apache Polaris
make setup
make up-polaris

# Get authentication token
make polaris-token

# Create a test catalog
make polaris-create-catalog
```

The setup includes:
- **Apache Polaris API** on port 8181
- **Apache Polaris Management** on port 8182
- PostgreSQL backend for metadata storage

### Configuration

Once Apache Polaris is running, configure your Lance Namespace client:

```python
from lance_namespace_impls.polaris import PolarisNamespace

ns = PolarisNamespace({
    "endpoint": "http://localhost:8181",
    "auth_token": "<token-from-make-polaris-token>"
})
```

## Next Steps

We are excited about this collaboration between the Lance and Apache Polaris communities.
Our integration with the Generic Table API opens up new possibilities for managing AI-native workloads in the open multimodal lakehouse.

Looking ahead, we plan to continue improving the Generic Table API based on our learnings from the Lance Namespace specification.
For example, Lance Namespace already supports credentials vending end-to-end and is 
integrated with any engine that uses the Lance Rust/Python/Java SDKs,
allowing namespace servers to provide temporary credentials for accessing table data.
We would like to collaborate with the Apache Polaris community to add credentials vending support to the Generic Table API,
enabling secure, fine-grained access control for Lance tables stored in AWS S3, Azure Blob Storage, and Google Cloud Storage.
This would allow Apache Polaris to fully manage credentials for Lance tables, just as it does for Iceberg tables today.

We welcome contributions and feedback from the community.
Join us in building the future of AI-native data management in the open multimodal lakehouse!

## Resources

- [What is a Multimodal Lakehouse](https://lancedb.com/blog/what-we-mean-by-multimodal/) - Rethinking What "Multimodal" Means for AI
- [Lance Format](https://github.com/lance-format/lance) - The Lance columnar format
- [LanceDB](https://github.com/lancedb/lancedb) - Developer-friendly open source embedded retrieval engine for multimodal AI.
- [Lance Namespace](https://github.com/lance-format/lance-namespace) - The namespace client specification
- [Lance Namespace Implementations](https://github.com/lance-format/lance-namespace-impls) - Catalog implementations including Apache Polaris
- [Lance Spark Connector](https://github.com/lance-format/lance-spark) - Apache Spark integration
- [Lance Ray](https://github.com/lance-format/lance-ray) - Lance Ray integration
- [Lance Trino Connector](https://github.com/lance-format/lance-trino) - Lance Trino integration
- [Lance Trino Connector](https://github.com/lance-format/lance-duckdb) - Lance DuckDB integration
