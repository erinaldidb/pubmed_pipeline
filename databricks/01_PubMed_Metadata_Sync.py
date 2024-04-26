# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PubMed MetaData Sync
# MAGIC
# MAGIC **Objective**: This notebook will syncronize the metadata of articles in PubMed provided in an S3 bucket with a delta table in Unity Catalog. This will provide a local reference for updates as well as provide a historical record of when publications were available for download.
# MAGIC
# MAGIC This notebook can be used interactively or as a script that can be used in a job. This notebook has the following sections that are all executed in series:
# MAGIC
# MAGIC  * **PubMed Pipline Application Config** - Standard for all PubMed Pipeline Notebooks. This will pull our pubmed configuration from `pubmed_pipeline_config` which is used by all notebooks in our pubmed pipeline application. This will ensure consistant configuration across all workflow tasks and organize verbose configurations elsewhere so that the notebook content is more task and less config oriented.
# MAGIC  * **Inspect UC Assets (OPTIONAL)** - CREATE IF NOT EXISTS sql is triggered anytime the `uc_name` is resolved in any of our pubmed pipline application assets. Thus, if the code for inspect is run and those assets don't exist, they will be created. To trigger the section, set `DISPLAY_CONFIGS` to `true`.
# MAGIC  * **`PUBMED_METADATA_TABLE` Streaming Merge** - This the the core job in the notebook which runs a streaming job to update `PUBMED_METADATA_TABLE`.
# MAGIC  * **Inspect `PUBMED_METADATA_TABLE` (OPTIONAL)** - Short validation code to inspect the changes since last update. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Widgets Configuration
dbutils.widgets.dropdown(name="FILE_TYPE", defaultValue="xml", choices=["xml", "text"])
FILE_TYPE = dbutils.widgets.get("FILE_TYPE")

# COMMAND ----------

# MAGIC %run ./_resources/pubmed_pipeline_config $RESET_ALL_DATA=false $DISPLAY_CONFIGS=true

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # `PUBMED_METADATA_TABLE` Streaming Upsert 
# MAGIC
# MAGIC For the ingest of PubMed metadata data into `PUBMED_METADATA_TABLE` we'll be using [Upsert from streaming queries using foreachBatch](https://docs.databricks.com/en/structured-streaming/delta-lake.html#upsert-from-streaming-queries-using-foreachbatch). However, there are quite a few configurations that go into this streaming process that we'll document below:
# MAGIC
# MAGIC  * **CloudFormat and Options** - We are going to [query a cloud storage object using autoloader](https://docs.databricks.com/en/query/streaming.html#query-data-in-cloud-object-storage-with-auto-loader). Thus, we will set the format to do this as `.format("cloudFiles")`. However, cloudFiles takes additional arguements, called *options*, to ensure that the csv source is read correctly. We'll set those configurations in the dict `readStream_options` and apply those configuration to the readStream using [`.options`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameReader.options.html).
# MAGIC  * **Load Source** - We'll use our notebook scope vaariable `PUBMED_SOURCE_METADATA_BUCKET` to [`.load`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.load.html) from our S3 bucket source.
# MAGIC  * **Select Columns** - While a common pattern is to ingest a data file raw and persist then run a second query that curates the source file, we are not going to do that for this metadata file. To avoid that unnecessary intermediate step, we are going to transform into our target table format using [`.select`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.select.html). This is the same select method that is availailable with regular pyspark dataframes. For readability, we are going to write our select columns as a list of pyspark columns in `readStream_columns` and pass as positional arguments into the `.select` method.
# MAGIC  * **WriteStream for each microbatch** - You are able to write a structured streaming dataframe by converting the streaming Dataframe to [`.writeStream`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.writeStream.html#pyspark-sql-dataframe-writestream) and the use [`.foreachBatch`](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html) to write each microbatch. `.foreachBatch` takes a function arguement, `upsert_metadata`. What's nice about this api design is that the function accepts microbatches as dataframes. Thus, the syntax that we use for streaming into delta tables is the same syntax that we use for batch merge jobs.
# MAGIC
# MAGIC **NOTE**: Gathering large arguments like this is not just helpful for readability, it also helps mitigate syntax errors by the developer. Since this streaming job was getting a little verbose, we applied the technique below.
# MAGIC
# MAGIC **NOTE**: The [trigger](https://docs.databricks.com/en/structured-streaming/triggers.html#configuring-incremental-batch-processing) setting of available sets the behavior to running as an incremental batch which makes sense because we are running this as a job once a day. 

# COMMAND ----------

# DBTITLE 0,Stream Configurations
from pyspark.sql import SparkSession, DataFrame, functions as F
from delta.tables import DeltaTable

# readStream Options:
readStream_options = {"cloudFiles.format": "csv",
                      "cloudFiles.allowOverwrites": "true",
                      "cloudFiles.schemaLocation": pubmed.raw_metadata.cp.path,
                      "header": "true"}

# pyspark dataframe columns to select from PubMed Metadata CloudFile
readStream_columns = [F.col("Key"),
                      F.col("ETag"),
                      F.col("Article Citation").alias("ArticleCitation"),
                      F.col("AccessionID"),
                      F.col("Last Updated UTC (YYYY-MM-DD HH:MM:SS)").cast("timestamp").alias("LastUpdated"),
                      F.col("PMID"),
                      F.col("License"),
                      F.col("Retracted"),
                      F.col("_metadata.file_path").alias("_file_path"),
                      F.col("_metadata.file_modification_time").alias("_file_modification_time"),
                      F.col("_metadata.file_size").alias("_file_size"),
                      F.current_timestamp().alias("_ingestion_timestamp"),
                      F.lit("PENDING").alias("status")]

def upsert_metadata(microBatchOutputDF: DataFrame, batchId: int):
    tgt_df = pubmed.raw_metadata.dt.alias("tgt")
    tgt_df.merge(source = microBatchOutputDF.alias("src"),
                 condition = "src.AccessionID = tgt.AccessionID") \
        .whenMatchedUpdateAll(condition="src.LastUpdated > tgt.LastUpdated") \
        .whenNotMatchedInsertAll() \
        .execute()

# COMMAND ----------

# TODO: Move PUBMED_SOURCE_METADATA_BUCKET into pubmed_central_utils
PUBMED_SOURCE_METADATA_BUCKET = f"s3://pmc-oa-opendata/oa_comm/{FILE_TYPE}/metadata/csv/"

spark.readStream.format("cloudFiles") \
                .options(**readStream_options) \
                .load(PUBMED_SOURCE_METADATA_BUCKET) \
                .select(*readStream_columns) \
    .writeStream.foreachBatch(upsert_metadata) \
                .trigger(availableNow=True) \
                .option("checkpointLocation", pubmed.raw_metadata.cp.path) \
                .queryName(f"query_{pubmed.raw_metadata.name}") \
                .start() \
                .awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Inspect `PUBMED_METADATA_TABLE` (OPTIONAL)
# MAGIC
# MAGIC Let's check out the history of the most recent versions.

# COMMAND ----------

inspect_metadata_hist=True
if inspect_metadata_hist:
    hist = spark.sql(f"DESCRIBE HISTORY {pubmed.raw_metadata.name}")
    display(hist)

# COMMAND ----------

inspect_metadata=True
if inspect_metadata:
    display(pubmed.raw_metadata.df)
