# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PubMed MetaData Sync
# MAGIC
# MAGIC This notebook can be used interactively or as a script that can be used in a job. The script has the following sections that are all executed in series:
# MAGIC
# MAGIC  * **Set Parameters Using Widgets (OPTIONAL)**
# MAGIC  * **Set PubMed Constants and Derived Variables**
# MAGIC  * **Define Utility Finctions**
# MAGIC  * **Run Streaming Merge Into MetaData Delta Table**
# MAGIC  * **Inspect Results (OPTIONAL)** 
# MAGIC
# MAGIC <h2> PUB MED CENTRAL - Metadata loader </h2>
# MAGIC
# MAGIC 1) Autoloader with allowOverwrites to download latest metadata filelist available from pubmed central
# MAGIC     - S3 Path: s3://pmc-oa-opendata/oa_comm/ for all the Commercial Use Allowed data
# MAGIC 2) Identify the differences with already ingested metadatas: identify new and retracted articles

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Set Notebook Arguments Using Widgets (OPTIONAL)
# MAGIC
# MAGIC This will enable interactive use of the notebook. Since we want our script to be executable across landscapes, we will argument script specific arguments which are:
# MAGIC
# MAGIC | Widget Variable | Description | Default Value |
# MAGIC | --------------- | ----------- | ------------- |
# MAGIC | `PUBMED_CATALOG` | The UC Catalog where we'll persist our PubMed Pipeline Volume Files, Tables, and Models | *pubmed_pipeline* |
# MAGIC | `PUBMED_SCHEMA` | The `PUBMED_CATALOG` schema where we'll persist our Raw Curation PubMed Volume Files and Tables | *raw* |
# MAGIC | `FILE_TYPE` | The file type that we want sync between Commercial Use Allowed Data and our local metadata table | *xml* |

# COMMAND ----------

# Reference code to configure widgets

set_widgets=False
if set_widgets:
    dbutils.widgets.text(name="PUBMED_CATALOG",
                         defaultValue="pubmed_pipeline",
                         label="Catalog for all Pubmed")
    dbutils.widgets.text(name="PUBMED_SCHEMA_RAW",
                         defaultValue="raw",
                         label="Schema for Raw File")
    dbutils.widgets.dropdown(name="FILE_TYPE",
                             defaultValue="xml",
                             choices=["xml", "text", "pdf", "all"],
                             label="Raw File ingest type")


# COMMAND ----------

# DBTITLE 1,INIT
#OA_COMM is for Commercial Use
#https://www.ncbi.nlm.nih.gov/pmc/tools/pmcaws/

# Widget Assigned Constants
PUBMED_CATALOG = dbutils.widgets.get("PUBMED_CATALOG")
PUBMED_SCHEMA_RAW = dbutils.widgets.get("PUBMED_SCHEMA_RAW")
FILE_TYPE = dbutils.widgets.get("FILE_TYPE")

# PubMed MetaData Constants
PMC_BUCKET = "s3://pmc-oa-opendata"
PMV_ROOT_PATH = "oa_comm/"

# Derived PubMed MetaData Sync Variables
volume_base_path = f"/Volumes/{PUBMED_CATALOG}/{PUBMED_SCHEMA_RAW}/articles"
checkpoint_path = f"{volume_base_path}/_checkpoints/"
metadata_table = f"{PUBMED_CATALOG}.{PUBMED_SCHEMA_RAW}.metadata_{FILE_TYPE}"

create_metadata_table_sql = \
f"""CREATE TABLE IF NOT EXISTS {metadata_table} (
    AccessionID STRING,
    LastUpdated TIMESTAMP,
    Status String, KEY String)
USING DELTA CLUSTER BY (AccessionID)"""

spark.sql(create_metadata_table_sql)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Utility functions
from delta.tables import *
import pyspark.sql.functions as fn

def upsertMetadata(df, epochId):
  delta_metadata = DeltaTable.forName(sparkSession=df.sparkSession.getActiveSession(),
                                      tableOrViewName=metadata_table).alias("target")
  delta_metadata.merge(df.alias("source"), "source.AccessionID = target.AccessionID") \
    .whenMatchedUpdateAll(condition="source.LastUpdated > target.LastUpdated") \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

# DBTITLE 1,Upsert Metadata table from latest CSV
list_df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.allowOverwrites", "true") \
  .option("cloudFiles.schemaLocation", checkpoint_path+metadata_table) \
  .option("header", "true") \
  .load(f"{PMC_BUCKET}/{PMV_ROOT_PATH}/{FILE_TYPE}/metadata/csv/") \
  .withColumnRenamed("Article Citation", "ArticleCitation") \
  .withColumnRenamed("Last Updated UTC (YYYY-MM-DD HH:MM:SS)", "LastUpdated") \
  .withColumn("LastUpdated", fn.col("LastUpdated").cast("timestamp")) \
  .withColumn("_file_path", fn.col("_metadata.file_path")) \
  .withColumn("_file_modification_time", fn.col("_metadata.file_modification_time")) \
  .withColumn("_file_size", fn.col("_metadata.file_size")) \
  .withColumn("_ingestion_timestamp", fn.current_timestamp()) \
  .withColumn("Status", fn.lit("PENDING"))

list_df \
  .writeStream \
  .foreachBatch(upsertMetadata) \
  .trigger(availableNow=True) \
  .option("checkpointLocation", checkpoint_path+metadata_table) \
  .queryName(f"query_{metadata_table}") \
  .start() \
  .awaitTermination()

# COMMAND ----------

inspect_metadata=False
if inspect_metadata:
    select_metadata_table_sql = f"SELECT * FROM {metadata_table}"
    metadata_df = spark.sql(select_metadata_table_sql)
    display(metadata_df.limit(10))
