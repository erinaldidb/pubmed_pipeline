# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook does a simple VS deployment to test if a workspace is properly configured for it's use.
# MAGIC
# MAGIC Before getting started, it is worth while to check out [Databricks VectorSearch](https://docs.databricks.com/en/generative-ai/vector-search.html) documentation.
# MAGIC
# MAGIC In the documentation you will see three different options to manage embeddings. We will use the first option:
# MAGIC  - **Sync Vector Index from Delta Table, Configure Index Sync with an embedding endpoint**
# MAGIC  - Sync Vector Index from Delta Table, Manage tokenization outside of VectorSearch
# MAGIC  - Don't sync Vector Index from Delta Table, Manage tokenization outside of VectorSearch
# MAGIC
# MAGIC  ---
# MAGIC
# MAGIC ## Create a delta source table with Content
# MAGIC
# MAGIC There are a couple pre-reqs that will need to be satisfied:
# MAGIC  * Install `databricks-vectorsearch`
# MAGIC  * Unity Catalog enabled workspace
# MAGIC  * Serverless compute enabled
# MAGIC  * Source table must have Change Data Feed enabled (See CREATE TABLE syntax below)
# MAGIC  * CREATE TABLE privileges on catalog schema(s) to create indexes.
# MAGIC  * Personal access tokens enabled.
# MAGIC
# MAGIC We are going to create the most simple of entries in table, `CONTENT_DELTA_TABLE_UC_NAME`, to test.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CONTENT_DELTA_TABLE_UC_NAME = "`pubmed-pipeline`.test.vs_test"
CONTENT_DELTA_TABLE_UC_NAME_NO_TICKS = CONTENT_DELTA_TABLE_UC_NAME.replace('`','')
VECTOR_SEARCH_ENDPOINT_NAME = "vector_search_test_hypen"
CONTENT_VS_INDEX_UC_NAME = CONTENT_DELTA_TABLE_UC_NAME + "_vs_index"
CONTENT_VS_INDEX_UC_NAME_NO_TICKS = CONTENT_VS_INDEX_UC_NAME.replace('`','')

# This is an easy way to make our table name available in the SQL API:
spark.conf.set("test.CONTENT_DELTA_TABLE_NAME", CONTENT_DELTA_TABLE_UC_NAME)
spark.conf.set("test.VECTOR_SEARCH_ENDPOINT_NAME", VECTOR_SEARCH_ENDPOINT_NAME)
spark.conf.set("test.CONTENT_VS_INDEX_UC_NAME", CONTENT_VS_INDEX_UC_NAME)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Dummy Source Table and populate with 3 disparate contents
# MAGIC CREATE TABLE IF NOT EXISTS ${test.CONTENT_DELTA_TABLE_NAME} (
# MAGIC     id BIGINT,
# MAGIC     content STRING)
# MAGIC CLUSTER BY (id) TBLPROPERTIES (delta.enableChangeDataFeed = true); 
# MAGIC
# MAGIC INSERT OVERWRITE ${test.CONTENT_DELTA_TABLE_NAME}
# MAGIC     VALUES (1, "Cars come in all colors including purple."),
# MAGIC            (2, "Cats can be big or small. Think about jungle tigers and kittens."),
# MAGIC            (3, "Petroleum producing states tend to have more wealth, but less diverse economies.");
# MAGIC
# MAGIC SELECT * FROM ${test.CONTENT_DELTA_TABLE_NAME};

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now that we have a source delta table created and populated, We can create our vector search
# MAGIC https://docs.databricks.com/en/generative-ai/create-query-vector-search.html
# MAGIC

# COMMAND ----------

# DBTITLE 1,VectorSearch helper functions to handle endpoint and index running state
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

import time
def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      if "REQUEST_LIMIT_EXCEEDED" in str(e):
        print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
        return
      else:
        raise e
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

def index_exists(vsc, endpoint_name, index_full_name):
    try:
        dict_vsindex = vsc.get_index(endpoint_name, index_full_name).describe()
        return dict_vsindex.get('status').get('ready', False)
    except Exception as e:
        if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
            print(f'Unexpected error describing the index. This could be a permission issue.')
            raise e
    return False

def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
  for i in range(180):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 40 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")

# COMMAND ----------

# This cell will create a Vector Search Index and await it's ready state. This can take a while, but only needs to be done once.
from databricks.vector_search.client import VectorSearchClient

# The following line automatically generates a PAT Token for authentication
client = VectorSearchClient()

if VECTOR_SEARCH_ENDPOINT_NAME not in [e['name'] for e in client.list_endpoints().get('endpoints', [])]:
    client.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME,
                           endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(client, VECTOR_SEARCH_ENDPOINT_NAME)

# COMMAND ----------

# This section adds an index to the endpoint

if not index_exists(client, VECTOR_SEARCH_ENDPOINT_NAME, CONTENT_VS_INDEX_UC_NAME_NO_TICKS):
  print(f"Creating index {CONTENT_VS_INDEX_UC_NAME_NO_TICKS} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  client.create_delta_sync_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
    index_name=CONTENT_VS_INDEX_UC_NAME_NO_TICKS,
    source_table_name=CONTENT_DELTA_TABLE_UC_NAME_NO_TICKS,
    pipeline_type="TRIGGERED",
    primary_key="id",
    embedding_source_column='content',                        #The column containing our text
    embedding_model_endpoint_name='databricks-bge-large-en' ) #The embedding endpoint used to create the embeddings
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  client.get_index(VECTOR_SEARCH_ENDPOINT_NAME, CONTENT_VS_INDEX_UC_NAME_NO_TICKS).sync()

#Let's wait for the index to be ready and all our embeddings to be created and indexed
wait_for_index_to_be_ready(client, VECTOR_SEARCH_ENDPOINT_NAME, CONTENT_VS_INDEX_UC_NAME_NO_TICKS)
print(f"index {CONTENT_VS_INDEX_UC_NAME_NO_TICKS} on table {CONTENT_DELTA_TABLE_UC_NAME_NO_TICKS} is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We are not going to test our endpoint using the same API we will use in an eventual RAG application.
# MAGIC
# MAGIC Choose one of the queries below to see if you get the expected content we loaded above.

# COMMAND ----------

QUERY = "What is an automobile?"
QUERY = "How is oil transported?"
QUERY = "Leo was a ThunderCat. What Color was his hair?"

index = client.get_index(endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME, index_name=CONTENT_VS_INDEX_UC_NAME_NO_TICKS)
rslt = index.similarity_search(num_results=1, columns=["content"], query_text=QUERY)
print(rslt['result']['data_array'][0][0])
