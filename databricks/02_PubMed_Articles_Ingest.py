# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # PubMed Articles Ingest
# MAGIC
# MAGIC **Objective**: This notebook will use both developer arguments, `pubmed.raw_metadata`, and `pubmed.raw_search_hist` to query PMC for new articles related to our key word search topcis of interest. Upon a successful run, `pubmed.raw_metadata` and `pubmed.raw_search_hist` will be updated with search and download metadata and articles will be downloaded to `pubmed.raw_articles`.
# MAGIC
# MAGIC
# MAGIC This notebook can be used interactively or as a script that can be used in a job. This notebook has a single section that runs a single method that only downloads files and updates metadata. As a convenience of execution, the method accepts pubmed asset classes.

# COMMAND ----------

# DBTITLE 1,Widget Configuration
dbutils.widgets.dropdown(name="FILE_TYPE", defaultValue="xml", choices=["xml", "text"])
FILE_TYPE = dbutils.widgets.get("FILE_TYPE")
dbutils.widgets.text(name="KW_SEARCH", defaultValue="brain cancer")
KW_SEARCH = dbutils.widgets.get("KW_SEARCH")
dbutils.widgets.dropdown(name="INSPECT_METADATA_HIST", defaultValue="true", choices=["true", "false"])
#INSPECT_METADATA_HIST = dbutils.widgets.get("INSPECT_METADATA_HIST")
dbutils.widgets.dropdown(name="INSPECT_SEARCH_HIST", defaultValue="true", choices=["true", "false"])
#INSPECT_SEARCH_HIST = dbutils.widgets.get("INSPECT_SEARCH_HIST")

# COMMAND ----------

# MAGIC %run ./_resources/pubmed_pipeline_config $RESET_ALL_DATA=false $DISPLAY_CONFIGS=true

# COMMAND ----------

# MAGIC %run ./_resources/pubmed_central_utils

# COMMAND ----------

# Example for adding a single keyword and searching for just that keyword:
#metadata_updates = get_needed_pmids_df(search_hist=pubmed.raw_search_hist,
#                                       metadata=pubmed.raw_metadata,
#                                       articles=pubmed.raw_articles,
#                                       keywords=dbutils.widgets.get("KW_SEARCH"))

# COMMAND ----------

# Example for updating all existing KW_SEARCH
metadata_updates = get_needed_pmids_df(search_hist=pubmed.raw_search_hist,
                                       metadata=pubmed.raw_metadata,
                                       articles=pubmed.raw_articles)

# COMMAND ----------

metadata_updates = get_needed_pmids_df(search_hist=pubmed.raw_search_hist,
                                       metadata=pubmed.raw_metadata,
                                       articles=pubmed.raw_articles,
                                       keywords="knee")

# COMMAND ----------

display(metadata_updates)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inspect `pubmed.raw_metadata` history

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pubmed_pipeline.raw.metadata_xml WHERE `status` = 'DOWNLOADED';

# COMMAND ----------

if dbutils.widgets.get("INSPECT_METADATA_HIST") == 'true':
    hist = spark.sql(f"DESCRIBE HISTORY {pubmed.raw_metadata.name}")
    display(hist)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inspect `pubmed.raw_search_hist`
# MAGIC
# MAGIC To avoid running unnecessary searches on PMC, we will persist our search history and update as we search over more and more date ranges. Since there isn't a known business use case where having a gap in search history is beneficial, raw_search_hist will only search over ranges with min max dates (no gaps in search). There is a helper function, `get_search_hist_args` that will expand a KW_SEARCH range requested by a user to avoid gaps.

# COMMAND ----------

if dbutils.widgets.get("INSPECT_METADATA_HIST") == 'true':
    dat = spark.sql(f"SELECT * FROM {pubmed.raw_search_hist.name}")
    display(dat)
