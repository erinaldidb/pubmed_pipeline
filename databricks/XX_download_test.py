# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This test it to determine if there are issues downloading from PubMed to a local tmp folder.

# COMMAND ----------

accession_id = 'PMC10839115'
file_type = "xml"

# COMMAND ----------

import boto3
from botocore import UNSIGNED
from botocore.client import Config  

s3_conn = boto3.client('s3', config=Config(signature_version=UNSIGNED))
try:
    s3_conn.download_file("pmc-oa-opendata",
                          f'oa_comm/{file_type}/all/{accession_id}.{file_type}',
                          f'/tmp/{accession_id}.{file_type}')
    print("DOWNLOADED")
except:
    print("ERROR")
