# Databricks notebook source
# MAGIC %pip install mlflow==2.9.0 lxml==4.9.3 transformers==4.30.2 langchain==0.0.344 databricks-vectorsearch==0.22
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init $reset_all_data=false

# COMMAND ----------

# TODO: include processed schema so that we can promote the final index as a processed

set_widgets=False
if set_widgets:
    dbutils.widgets.text(name="PUBMED_CATALOG",
                         defaultValue="pubmed_pipeline",
                         label="Catalog for all Pubmed")
    dbutils.widgets.text(name="PUBMED_SCHEMA_RAW",
                         defaultValue="raw",
                         label="Schema for Raw File")
    dbutils.widgets.text(name="PUBMED_SCHEMA_STAGE",
                         defaultValue="stage",
                         label="Schema for Stage File")    
    dbutils.widgets.dropdown(name="FILE_TYPE",
                             defaultValue="xml",
                             choices=["xml", "text", "pdf", "all"],
                             label="Raw File ingest type")
    dbutils.widgets.text(name="VECTOR_SEARCH_ENDPOINT_NAME",
                         defaultValue="pubmed",
                         label="Name of PubMed endpoint")
    dbutils.widgets.text(name="PUBMED_DOCS_VOLUME",
                         defaultValue="articles")

# COMMAND ----------

catalog = dbutils.widgets.get("PUBMED_CATALOG")
PUBMED_CATALOG = catalog
PUBMED_SCHEMA_RAW = dbutils.widgets.get("PUBMED_SCHEMA_RAW")
PUBMED_SCHEMA_STAGE = dbutils.widgets.get("PUBMED_SCHEMA_STAGE")
FILE_TYPE = dbutils.widgets.get("FILE_TYPE")
VECTOR_SEARCH_ENDPOINT_NAME = dbutils.widgets.get("VECTOR_SEARCH_ENDPOINT_NAME")
PUBMED_DOCS_VOLUME = dbutils.widgets.get("PUBMED_DOCS_VOLUME")

# COMMAND ----------

# TODO: add code in init to also handle the creation of the pubmed volume, default to managed
# For now we will have to create manually if it is not already completed.
volume_base_path = f"/Volumes/{PUBMED_CATALOG}/{PUBMED_SCHEMA_STAGE}/{PUBMED_DOCS_VOLUME}"
checkpoint_path = f"{volume_base_path}/_checkpoints/"
documents_path = f"{volume_base_path}/docs/"

metadata_table = f"{PUBMED_CATALOG}.{PUBMED_SCHEMA_STAGE}.metadata_{FILE_TYPE}"
documents_table = f"{PUBMED_CATALOG}.{PUBMED_SCHEMA_STAGE}.doc_data_{FILE_TYPE}"
documents_chunks_table = f"{PUBMED_CATALOG}.{PUBMED_SCHEMA_STAGE}.doc_chunks_{FILE_TYPE}"

repartition_param = 128

# COMMAND ----------

# DBTITLE 1,Split docs in chunks and test it
from langchain.text_splitter import HTMLHeaderTextSplitter, RecursiveCharacterTextSplitter, CharacterTextSplitter
from transformers import AutoTokenizer

max_chunk_size = 500
chunk_overlap = 50

#HOW THE TABLES WILL BE PROCESSED??
#HOW THE MATH EXPRESSIONS WILL BE PROCESSED??

tokenizer = AutoTokenizer.from_pretrained("hf-internal-testing/llama-tokenizer")
text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(tokenizer, chunk_size=max_chunk_size, chunk_overlap=50)
html_splitter = HTMLHeaderTextSplitter(headers_to_split_on=[("p", "paragraph")])
char_splitter = CharacterTextSplitter(
    separator=". ",
    chunk_size=max_chunk_size,
    chunk_overlap=chunk_overlap,
    length_function=len,
    is_separator_regex=False,
    keep_separator=False
)

# Split on H2, but merge small h2 chunks together to avoid too small. 
def split_html_on_h2(html, min_chunk_size = 20, max_chunk_size=500):
  if not html:
      return []
  page_chunk = html_splitter.split_text(html)
  chunks = []
  previous_chunk = ""
  # Merge chunks together to add text before h2 and avoid too small docs.
  for x in page_chunk:
    # Concat the h2 (note: we could remove the previous chunk to avoid duplicate h2)
    content = x.metadata.get('paragraph', "") + "\n" + x.page_content
    sub_chunk = char_splitter.split_text(content)
    for c in sub_chunk:
        if len(tokenizer.encode(previous_chunk + c)) <= max_chunk_size/2:
            previous_chunk += c + "\n"
        else:
            chunks.extend(text_splitter.split_text(previous_chunk.strip()))
            previous_chunk = c + "\n"
    if previous_chunk:
        chunks.extend(text_splitter.split_text(previous_chunk.strip()))
  # Discard too small chunks
  return [c for c in chunks if len(tokenizer.encode(c)) > min_chunk_size]

# COMMAND ----------

import pyspark.sql.functions as fn
import pandas as pd

# Let's create a user-defined function (UDF) to chunk all our documents with spark
@fn.pandas_udf("array<string>")
def parse_and_split(docs: pd.Series) -> pd.Series:
    return docs.apply(split_html_on_h2)
    
spark.readStream.option("skipChangeCommits", "true").table(documents_table) \
    .select("accessionId","body","pdf_link") \
    .withColumnRenamed("accessionId", "accession_id") \
    .filter('body is not null') \
    .repartition(repartition_param) \
    .withColumn('content', fn.explode(parse_and_split('body'))) \
    .drop("body") \
    .writeStream \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpoint_path+documents_chunks_table) \
    .queryName(f"query_{documents_chunks_table}") \
    .toTable(documents_chunks_table) \
    .awaitTermination()

# COMMAND ----------

# This can take a while, be prepared to go get some coffee... ~ 15 minutes.
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

if VECTOR_SEARCH_ENDPOINT_NAME not in [e['name'] for e in vsc.list_endpoints().get('endpoints', [])]:
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME)
print(f"Endpoint named {VECTOR_SEARCH_ENDPOINT_NAME} is ready.")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

#The table we'd like to index
source_table_fullname = documents_chunks_table
# Where we want to store our index
vs_index_fullname = f"{documents_chunks_table}_vs_index"

if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  vsc.create_delta_sync_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
    index_name=vs_index_fullname,
    source_table_name=source_table_fullname,
    pipeline_type="TRIGGERED",
    primary_key="id",
    embedding_source_column='content', #The column containing our text
    embedding_model_endpoint_name='databricks-bge-large-en' #The embedding endpoint used to create the embeddings
  )
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).sync()

#Let's wait for the index to be ready and all our embeddings to be created and indexed
wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
print(f"index {vs_index_fullname} on table {source_table_fullname} is ready")
