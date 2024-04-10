# Databricks notebook source
# MAGIC %md
# MAGIC <h1> PUB MED CENTRAL Loader </h1>
# MAGIC
# MAGIC 1) Autoloader with allowOverwrites to download latest metadata filelist available from pubmed central
# MAGIC     - S3 Path: s3://pmc-oa-opendata/oa_comm/ for all the Commercial Use Allowed
# MAGIC 2) Identify the differences with already ingested metadatas: identify new records to add and retracted to be removed
# MAGIC 3) Query PubMedCentral with keywords using the api available in http://eutils.ncbi.nlm.nih.gov
# MAGIC     - **Example: Retrieve all HPV related docs from PMC** <br> https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&usehistory=y&term=hpv[kwd]
# MAGIC
# MAGIC 4) Mark retrieved documents in metadata table
# MAGIC 5) Download and process documents 

# COMMAND ----------

keyword_search = dbutils.widgets.get("keyword_search")

# COMMAND ----------

# DBTITLE 1,INIT
#OA_COMM is for Commercial Use
#https://www.ncbi.nlm.nih.gov/pmc/tools/pmcaws/

pmc_bucket = "s3://pmc-oa-opendata"
base_path = "oa_comm/"

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

volume_base_path = f"/Volumes/{catalog}/{schema}/articles"
checkpoint_path = f"{volume_base_path}/_checkpoints"
documents_path = f"{volume_base_path}/docs/"

file_type = "txt"

metadata_table = f"{catalog}.{schema}.metadata"
documents_table = f"{catalog}.{schema}.doc_data"

spark.sql(f"CREATE TABLE IF NOT EXISTS {metadata_table} (AccessionID STRING, LastUpdated TIMESTAMP, Status String, KEY String) USING DELTA CLUSTER BY (AccessionID)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {documents_table} (AccessionID STRING, LastUpdated TIMESTAMP) USING DELTA CLUSTER BY (AccessionID)")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

#Cleanup and reset markers
#spark.sql(f"truncate table {metadata_table}")
#spark.sql(f"truncate table {documents_table}")
#dbutils.fs.rm("/Volumes/{catalog}/{schema}/landing/all", True)
#dbutils.fs.rm(checkpoint_path, True)
#dbutils.fs.mkdirs("/Volumes/{catalog}/{schema}/landing/all/xml")
#dbutils.fs.mkdirs("/Volumes/{catalog}/{schema}/landing/all/txt")

#spark.sql("update ema_rina.pub_med.metadata set Status = 'PENDING'")

# COMMAND ----------

# DBTITLE 1,Utility functions
from delta.tables import *
import pyspark.sql.functions as fn
from typing import Iterator
import pandas as pd

def upsertMetadata(df, epochId):
  delta_metadata = DeltaTable.forName(sparkSession=df.sparkSession.getActiveSession(), tableOrViewName=metadata_table).alias("target")
  
  delta_metadata.merge(df.alias("source"), "source.AccessionID = target.AccessionID") \
  .whenMatchedUpdateAll(condition="source.LastUpdated > target.LastUpdated") \
  .whenNotMatchedInsertAll() \
  .execute()

@udf
def downloaderUDF(accession_id: str, file_type:str = "txt"):
  import boto3
  from botocore import UNSIGNED
  from botocore.client import Config

  s3_conn = boto3.client('s3', config=Config(signature_version=UNSIGNED))
  bucket_name = "pmc-oa-opendata"
  prefix = f"oa_comm/{file_type}/all"
  try:
    s3_conn.download_file(bucket_name, f'{prefix}/{accession_id}.{file_type}', f"{volume_base_path}/all/{file_type}/{accession_id}.{file_type}")
    return "DOWNLOADED"
  except:
    return "ERROR"
  
@udf
def pmcReaderUDF(key: str):
  import boto3
  from botocore import UNSIGNED
  from botocore.client import Config

  s3_conn = boto3.client('s3', config=Config(signature_version=UNSIGNED))
  bucket_name = "pmc-oa-opendata"
  prefix = f"oa_comm/{file_type}/all"
  
  s3_object = s3_conn.get_object(Bucket=bucket_name, Key=key)
  body = s3_object['Body']
  return body.read().decode("utf-8", errors='ignore')

def extractDoc(doc_col: str):
  def transform(df):
    return (
      df.withColumn("parsed", fn.split(doc_col, "\n==== "))
        .withColumn("Front", fn.replace(fn.element_at("parsed", 2),fn.lit("Front\n"),fn.lit("")))
        .withColumn("Body", fn.replace(fn.element_at("parsed", 3),fn.lit("Body\npmc"),fn.lit("")))
        .withColumn("Refs", fn.replace(fn.element_at("parsed", 4),fn.lit("Refs\n"),fn.lit("")))
        .withColumn("pdf_link", fn.concat(fn.lit('https://www.ncbi.nlm.nih.gov/pmc/articles/'),"AccessionID",fn.lit('/pdf')))
        #Not a reliable solution to fetch figures
        #.withColumn("figures", fn.array_distinct(fn.regexp_extract_all("Body", fn.lit('(Fig(.)?(ure)?[s]?.?([\\d]+))'),4)))
        #.withColumn("figure_links", fn.transform("figures", lambda x: fn.concat(fn.lit("https://www.ncbi.nlm.nih.gov/pmc/articles/"),"AccessionID",fn.lit("/figure/F"),x)))
        .drop(doc_col,"parsed")
    )
  return transform


# COMMAND ----------

# DBTITLE 1,Upsert Metadata table from latest CSV
list_df = (
  spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "csv")
           .option("cloudFiles.allowOverwrites", "true")
           .option("cloudFiles.schemaLocation", checkpoint_path)
           .option("header", "true")
           .load(f"{pmc_bucket}/{base_path}/{file_type}/metadata/csv/")
  )

list_df = (
  list_df
    .withColumnRenamed("Article Citation", "ArticleCitation")
    .withColumnRenamed("Last Updated UTC (YYYY-MM-DD HH:MM:SS)", "LastUpdated")
    .withColumn("LastUpdated", fn.col("LastUpdated").cast("timestamp"))
    .withColumn("_file_path", fn.col("_metadata.file_path"))
    .withColumn("_file_modification_time", fn.col("_metadata.file_modification_time"))
    .withColumn("_file_size", fn.col("_metadata.file_size"))
    .withColumn("_ingestion_timestamp", fn.current_timestamp())
    .withColumn("Status", fn.lit("PENDING"))
  )

list_df.writeStream.foreachBatch(upsertMetadata).trigger(availableNow=True).option("checkpointLocation", checkpoint_path).start().awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> SEARCH FOR PAPERS AND DOWNLOAD RESULTS </h1>

# COMMAND ----------

# DBTITLE 1,Search PMCIDs in PMC based on keywords
import requests
import defusedxml.ElementTree as ET
from time import sleep

#database info and filters
#https://eutils.ncbi.nlm.nih.gov/entrez/eutils/einfo.fcgi?db=pmc

#Search Example
#https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&usehistory=y&term=hpv[kwd]

base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&usehistory=y"
ret_max = 10000
date_range = "mindate=2022/01/01&maxdate=2024/03/31"

final_url = f"{base_url}&term={keyword_search}&{date_range}&retmax={ret_max}"

req = requests.get(final_url)
xml_result = req.text

tree = ET.fromstring(xml_result)

count = int(tree.findtext("Count"))
web_env = tree.findtext("WebEnv")
query_key = tree.findtext("QueryKey")

list_pmid = []
for i in range(0, count, ret_max):
  final_url = f"{base_url}&term={keyword_search}&{date_range}&retmax={ret_max}&retstart={i}"
  req = requests.get(final_url)
  xml_result = req.text
  tree = ET.fromstring(xml_result)
  for ids in tree.find("IdList"):
    list_pmid.append("PMC"+ids.text)
  sleep(0.5) #limit 2 api calls/sec

print("TOTAL RESULTS:",len(list_pmid))

#TESTING WITH LIMIT
list_pmid = list_pmid[:15000]

# COMMAND ----------

# DBTITLE 1,MARK retrieved documents and UPDATE Metadata
metadata_df = spark.read.table(metadata_table)

list_pmid_df = spark.createDataFrame(list_pmid, "string").toDF("AccessionID")

#GetAndParse document
to_merge_df = (metadata_df
  .filter("Status == 'PENDING'") 
  .join(list_pmid_df, "AccessionID", "inner")
)

delta_metadata = DeltaTable.forName(sparkSession=spark, tableOrViewName=metadata_table).alias("target")

delta_metadata.merge(to_merge_df.select("AccessionID","Status").alias("source"), "source.AccessionID = target.AccessionID") \
  .whenMatchedUpdate(set={"Status": fn.lit("MARKED")}) \
  .execute()

# COMMAND ----------

# DBTITLE 1,Insert new DOCS and Delete Retracted in DOC_DATA
delta_docs = DeltaTable.forName(sparkSession=spark, tableOrViewName=documents_table).alias("target")

#APPEND NEW DOCS
(metadata_df.filter("Status == 'MARKED' and Retracted == 'no'")
  .join(list_pmid_df, "AccessionID", "inner")
  .join(delta_docs.toDF().select("AccessionID"), "AccessionID", "left_anti") #DO NOT PROCESS ALREADY INGESTED!
  .repartition(64)
  .withColumn("doc_content", pmcReaderUDF(fn.col("Key")))
  .transform(extractDoc("doc_content"))
  .drop("_rescued_data","_file_path","_file_modification_time","_file_size","_ingestion_timestamp", "Status")
).write.option("mergeSchema", "true").mode("append").saveAsTable(documents_table)

#REMOVED RETRACTED
delta_docs.merge(metadata_df.alias("source").filter("Status == 'MARKED' and Retracted ='yes'"),
  "source.AccessionID = target.AccessionID") \
  .whenMatchedDelete() \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ema_rina.pub_med.metadata
# MAGIC --select COUNT(*) AS DOCS from ema_rina.pub_med.doc_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ema_rina.pub_med.doc_data

# COMMAND ----------

# MAGIC %md
# MAGIC <H1> CHUNK STEPS

# COMMAND ----------

pattern = r"creativecommons.*?\n(.*?)\n"

df = spark.read.table(documents_table)\
    .withColumn("abstract", fn.regexp_extract("Front", pattern, 1))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC https://www.cbioportal.org/study/summary?id=aml_target_2018_pub

# COMMAND ----------


