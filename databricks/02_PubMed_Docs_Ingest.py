# Databricks notebook source
import inspect

# COMMAND ----------

displayHTML("<b>HELLO</b>")

# COMMAND ----------

import inspect

def display_html_outer(str):
    inspect.stack()[0].frame.f_globals['displayHTML'](str)

display_html_outer("<b>HELLO</b>")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2> PUB MED CENTRAL - Documents loader </h2>
# MAGIC
# MAGIC 1) Info about the parameters
# MAGIC     - **keyword_search** can be a single value or a Volumes path for a list of keywords with new line for each element.
# MAGIC     - **limit_results** limits the results for each keyword. If **0** then all the results will be processed.
# MAGIC
# MAGIC 2) Query PubMedCentral with keywords using the api available in http://eutils.ncbi.nlm.nih.gov
# MAGIC     - **Example: Retrieve all HPV related docs from PMC** <br> https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&usehistory=y&term=hpv[kwd]
# MAGIC
# MAGIC 3) Mark retrieved documents in metadata table
# MAGIC 4) Download and process documents 
# MAGIC 5) Remove all the retracted
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # PubMed Articles Ingest
# MAGIC
# MAGIC **Objective**: In the previous notebook we got a listing of all articles. Since we will likely want to focus on a group of articles, in this notebook we will run a key word search to get a subset of articles of our interest. Then we will proceed in downloading them to raw and read into curated.
# MAGIC
# MAGIC This notebook can be used interactively or as a script that can be used in a job. This notebook has the following sections that are all executed in series:
# MAGIC
# MAGIC  * **PubMed Pipline Application Config** - Standard for all PubMed Pipeline Notebooks. This will pull our pubmed configuration from a config notebook which will be used by all notebooks in the pubmed application.
# MAGIC  * **Set notebook variables** - Standard for all PubMed Pipeline Notebooks. These are arguments we want exposed for scheduling jobs and have notebook scope.
# MAGIC  * **Run `get_keyword_pmids`** - This method will provide a python set of `pmids` which are a unique identifier for PubMed articles.
# MAGIC  * **Run `get_pending_pmids`** - This method will run a query against `PUBMED_METADATA_TABLE` to find all pmids that satisfy `status ='PENDING'`.
# MAGIC  * **Create `ingest_pmids_df` pyspark DataFrame** - Sine we have `keyword_pmids` from `get_keyword_pmids` and `pending_pmids` from `get_pending_pmids`, we can now create a dataframe of the pmid we need to ingest, `pmids_df`.
# MAGIC  * **Run `get_needed_pmids`** - PUBMED_METADATA_TABLE Streaming Merge 
# MAGIC
# MAGIC  * **CREATE `PUBMED_METADATA_TABLE`** - We will run a create metadata table method
# MAGIC  * **`PUBMED_METADATA_TABLE` Streaming Merge** - This the the core job in the notebook which runs a streaming job to update `PUBMED_METADATA_TABLE`.
# MAGIC  * **Inspect `PUBMED_METADATA_TABLE` (OPTIONAL)** - Short validation code to inspect the changes since last update. 
# MAGIC

# COMMAND ----------

# MAGIC %run ./_resources/pubmed_config $reset_all_data=false

# COMMAND ----------

keyword_search = "brain cancer"
pmids = get_keyword_pmids(keyword_search, limit_results=20)
pmids

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **TODO** - Actually record / retain the xml files and leave in raw schema.
# MAGIC
# MAGIC **TODO** - Write the table form of the article as a **curated** schema table (currently written as a raw table) 

# COMMAND ----------

# Reference code to configure widgets

set_widgets=True
if set_widgets:
    dbutils.widgets.dropdown(name="FILE_TYPE",
                             defaultValue="xml",
                             choices=["xml", "text", "pdf", "all"],
                             label="Raw File ingest type")
    dbutils.widgets.dropdown(name="KW_SEARCH",
                             defaultValue="Brain Cancer",
                             choices=["Brain Cancer",],
                             label="KW Argument for PubMed Search")
    dbutils.widgets.dropdown(name="RSLT_LIMIT",
                             defaultValue="20",
                             choices=["10","20","50","200","1000","20000","500000", "0"],
                             label="Raw File ingest type")
    
FILE_TYPE = dbutils.widgets.get("FILE_TYPE")
KW_SEARCH = dbutils.widgets.get("KW_SEARCH")
RSLT_LIMIT = int(dbutils.widgets.get("RSLT_LIMIT"))


# COMMAND ----------

# DBTITLE 1,INIT
#OA_COMM is for Commercial Use
#https://www.ncbi.nlm.nih.gov/pmc/tools/pmcaws/

pmc_bucket = "s3://pmc-oa-opendata"
base_path = "oa_comm/"


volume_base_path = f"/Volumes/{PUBMED_CATALOG}/{PUBMED_DOCS_VOLUME}/pub_med_vol"
metadata_table = f"{catalog}.{schema}.metadata_{file_type}"
documents_table = f"{catalog}.{schema}.doc_data_{file_type}"
documents_chunks_table = f"{catalog}.{schema}.doc_chunks_{file_type}"



# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {documents_table} (
    AccessionID STRING, 
    LastUpdated TIMESTAMP
) USING DELTA CLUSTER BY (AccessionID)""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {documents_chunks_table} (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY,
  accession_id STRING,
  pdf_link STRING,
  content STRING
) CLUSTER BY (accession_id) TBLPROPERTIES (delta.enableChangeDataFeed = true); 
""")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------



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

# COMMAND ----------

# DBTITLE 1,Utility functions
from delta.tables import *
import pyspark.sql.functions as fn
from typing import Iterator
import pandas as pd

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
  import html

  s3_conn = boto3.client('s3', config=Config(signature_version=UNSIGNED))
  bucket_name = "pmc-oa-opendata"
  prefix = f"oa_comm/{file_type}/all"
  
  s3_object = s3_conn.get_object(Bucket=bucket_name, Key=key)
  body = s3_object['Body']
  
  return html.unescape(body.read().decode("utf-8", errors='ignore'))

def extractDoc(doc_col: str):
  def transform(df):
    return (
      df.withColumn("parsed", fn.split(doc_col, "\n==== "))
        .withColumn("Front", fn.replace(fn.element_at("parsed", 2),fn.lit("Front\n"),fn.lit("")))
        .withColumn("Body", fn.replace(fn.element_at("parsed", 3),fn.lit("Body\npmc"),fn.lit("")))
        .withColumn("Refs", fn.replace(fn.element_at("parsed", 4),fn.lit("Refs\n"),fn.lit("")))
        .withColumn("pdf_link", fn.concat(fn.lit('https://www.ncbi.nlm.nih.gov/pmc/articles/'),"AccessionID",fn.lit('/pdf')))
        .drop(doc_col,"parsed")
    )
  return transform

def extractXML(doc_col: str):
  def transform(df):
    return (
    df.withColumn("front", fn.regexp_extract(doc_col, r"<front>[\s\S]*?<\/front>", 0))

      .withColumn("title", fn.regexp_extract(doc_col, r"<article-title[\s\S]*?>([\s\S]*?)<\/article-title>", 1))
      .withColumn("abstract", fn.regexp_extract(doc_col, r"<abstract[\s\S]*?>([\s\S]*?)<\/abstract>", 1))
      .withColumn("body", fn.regexp_extract(doc_col, r"<body[\s\S]*?>([\s\S]*?)<\/body>", 1))
      .withColumn("back", fn.regexp_extract(doc_col, r"<back[\s\S]*?>([\s\S]*?)<\/back>", 1))

      .withColumn("figures", fn.regexp_extract_all(doc_col, fn.lit(r"<graphic[\s\S]*?href=\".*?([\s\S]*?)\"")))
      .withColumn("figure_links", fn.transform("figures", lambda x: fn.concat(fn.lit("https://www.ncbi.nlm.nih.gov/pmc/articles/"),"AccessionID",fn.lit("/bin/"),x,fn.lit(".jpg"))))
      .withColumn("pdf_link", fn.concat(fn.lit('https://www.ncbi.nlm.nih.gov/pmc/articles/'),"AccessionID",fn.lit('/pdf')))

      .withColumn("body_chunks", fn.split(fn.regexp_replace("body", "<p[\s\S]*?>([\s\S]*?)</p>", r"$1==SPLIT!=="), "==SPLIT!=="))
      .withColumn("body_chunks_clean", fn.transform("body_chunks", lambda x: fn.regexp_replace(fn.regexp_replace(x, '<.*?>',""), '&#x.*?;',""))
      ).drop(doc_col,"frontParsed","figures","body_chunks")
    )
  return transform

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

#Paper in XML format
#https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=PMC10854087

keyword_search = "TAN"
limit_results = 20 


def searchPMCPapers(keyword: str):
  base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&usehistory=y"
  ret_max = 10000
  date_range = "mindate=2022/01/01&maxdate=2024/03/31"

  final_url = f"{base_url}&term={keyword}&{date_range}&retmax={ret_max}"

  req = requests.get(final_url)
  xml_result = req.text

  tree = ET.fromstring(xml_result)

  count = int(tree.findtext("Count"))
  web_env = tree.findtext("WebEnv")
  query_key = tree.findtext("QueryKey")

  pmids = set()

  for i in range(0, count, ret_max):
    final_url = f"{base_url}&term={keyword}&{date_range}&retmax={ret_max}&retstart={i}"
    req = requests.get(final_url)
    xml_result = req.text
    tree = ET.fromstring(xml_result)
    for ids in tree.find("IdList"):
      pmids.add("PMC"+ids.text)
    sleep(0.5) #limit 2 api calls/sec
  
  print(f"{len(pmids)} papers found for '{keyword}'")
  if limit_results:
    return list(pmids)[:limit_results]
  else:
    return pmids

list_pmid = set()

if("/Volumes/" in keyword_search):
  with open(keyword_search, 'r') as keywords:
    for keyword in keywords.readlines():
      pmids = searchPMCPapers(keyword.strip())
      list_pmid.update(pmids)
else:
  pmids = searchPMCPapers(keyword_search.strip())
  list_pmid.update(pmids)

if limit_results:
  print(f"LIMITED RESULTS TO {limit_results} PER REQUEST")

print("TOTAL RESULTS TO PROCESS:",len(list_pmid))

# COMMAND ----------

list_pmid

# COMMAND ----------

# DBTITLE 1,MARK retrieved documents and UPDATE Metadata
metadata_df = spark.read.table(metadata_table)

list_pmid_df = spark.createDataFrame(list_pmid, "string").toDF("AccessionID")

#GetAndParse document
to_merge_df = (metadata_df
  .filter("Status == 'PENDING'") 
  .join(list_pmid_df, "AccessionID", "inner")
  .select("AccessionID","Status")
)

delta_metadata = DeltaTable.forName(sparkSession=spark, tableOrViewName=metadata_table).alias("target")

delta_metadata.merge(to_merge_df.alias("source"), "source.AccessionID = target.AccessionID") \
  .whenMatchedUpdate(set={"Status": fn.lit("MARKED")}) \
  .execute()

# COMMAND ----------

# DBTITLE 1,Insert new DOCS and Delete Retracted in DOC_DATA
#APPEND NEW DOCS
(metadata_df.filter("Status == 'MARKED' and Retracted == 'no'")
  .join(list_pmid_df, "AccessionID", "inner")
  .join(spark.read.table(documents_table).select("AccessionID"), "AccessionID", "left_anti") #DO NOT PROCESS ALREADY INGESTED!
  .repartition(64)                                                                           #Forcing multiple workers to jump in and download data
  .withColumn("doc_content", pmcReaderUDF(fn.col("Key")))
  .transform(extractXML("doc_content"))
  .drop("_rescued_data","_file_path","_file_modification_time","_file_size","_ingestion_timestamp", "Status")
).write.option("mergeSchema", "true").mode("append").saveAsTable(documents_table)

#REMOVE RETRACTED FROM docs
delta_docs = DeltaTable.forName(sparkSession=spark, tableOrViewName=documents_table).alias("target")
delta_docs.merge(metadata_df.alias("source").filter("Status == 'MARKED' and Retracted ='yes'"),
  "source.AccessionID = target.AccessionID") \
  .whenMatchedDelete() \
  .execute()

#REMOVE RETRACTED FROM docs_chunks
delta_docs_chunks = DeltaTable.forName(sparkSession=spark, tableOrViewName=documents_chunks_table).alias("target")
delta_docs_chunks.merge(metadata_df.alias("source").filter("Status == 'MARKED' and Retracted ='yes'"),
  "source.AccessionID = target.accession_id") \
  .whenMatchedDelete() \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Working With Delta Table Classes (OPTIONAL)
# MAGIC
# MAGIC It is worth understanding the difference between these two classes:
# MAGIC  * [pyspark DataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html#pyspark-sql-dataframe) - `pyspark.sql.DataFrame` is the Spark Dataframe class for the pyspark python API. It is a local abstraction of a DataFrame meaning that it has all the transforms defined by user, but doesn't actually have that data in the local driver session. It is instead lazily evaluated - that means it is not until a DataFrame action, like write or display, that the transforms are actually executed.
# MAGIC  * [delta DataFrame](https://docs.delta.io/latest/api/python/spark/index.html#module-delta.tables) - `delta.tables.DeltaTable` is the Delta Table class for the delta python API. This is a definition of a delta table that has already been registered, thus it will have DeltaLake metadata and table methods. Specifically, it includes the method `merge` which is the recommended, high performance, way to write into delta storage.
# MAGIC
# MAGIC  **NOTE**: It is a very common practice to inspect your delta tables as you are developing. To display the table interactively in a Databricks notebook, you must first get a Spark DataFrame class from the respective DeltaTable class. This is simple using the `toDF` method. Alternately, you can get the table using the built in pyspark API method, `table`, where you pass the table name as a string.

# COMMAND ----------

run_interactive = True
if run_interactive:
    display(spark.table(documents_table))

# COMMAND ----------

if run_interactive:
    display(DeltaTable.forName(sparkSession = spark, tableOrViewName=documents_table).toDF())
