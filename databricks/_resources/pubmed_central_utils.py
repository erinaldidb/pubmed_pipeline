# Databricks notebook source
# This notebook is to consolidate larger, complex functions used to interact with pubmed that will be used in the extract job
from delta.tables import *
import pyspark.sql.functions as fn
from typing import Iterator
import pandas as pd
import requests
import defusedxml.ElementTree as ET
from time import sleep
from datetime import datetime
from functools import cached_property

# COMMAND ----------

from requests import Response
from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Union, List
from delta.tables import DeltaTable

@dataclass
class PaperSearchResponse:
    response: Response

    def __post_init__(self):
        # Coerse response text into Element Tree
        self.response_tree = ET.fromstring(self.response.text)
        self.count = int(self.response_tree.findtext("Count"))
        self.web_env = self.response_tree.findtext("WebEnv")
        self.query_key = self.response_tree.findtext("QueryKey")
        self.pmids = set(["PMC"+id.text for id in self.response_tree.find("IdList")])


def searchPMCPapers(keyword: str,
                    min_dte: str = "2022/01/01",
                    max_dte: Optional[str] = None,
                    ret_max: int = 5000) -> {str}:
    """Search to retrieve docs"""
    date_range = f"mindate={min_dte}&maxdate={max_dte or datetime.today().strftime('%Y/%m/%d')}"
    SEARCH_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&usehistory=y"
    SEARCH_PAGE_URL = f"{SEARCH_BASE_URL}&term={keyword}&{date_range}&retmax={ret_max}" + "&retstart={i_pmid}"

    response = PaperSearchResponse(requests.get(SEARCH_PAGE_URL.format(i_pmid=0)))
    pmids = response.pmids
    
    for i_pmid in range(ret_max, response.count, ret_max):
        sleep(0.5) #limit 2 api calls/sec
        response = PaperSearchResponse(requests.get(SEARCH_PAGE_URL.format(i_pmid=i_pmid)))
        pmids |= response.pmids

    return pmids

# COMMAND ----------

from pyspark.sql.functions import col, lit, least

# Get a date range that will avoid creating gaps in search range from previous runs

def get_search_hist_args(keywords: Union[str, List[str]],
                         search_hist: PubMedAsset,
                         min_dte: str = "2022/01/01",
                         max_dte: Optional[str] = None) -> List[dict]:
    keywords: [str] = keywords if isinstance(keywords, list) else [str(keywords),]
    max_dte = max_dte or datetime.today().strftime('%Y/%m/%d')
    search_args_df = search_hist._spark.createDataFrame(data = [(kw.lower(), min_dte, max_dte) for kw in keywords],
                                                                     schema = "keyword STRING, min_dte STRING, max_dte STRING")
    args = search_args_df.alias('a').join(search_hist.df.alias('h'), "keyword", "outer") \
                         .withColumn('h_min', F.coalesce(F.col('h.min_dte'), F.col('a.min_dte'))) \
                         .withColumn('h_max', F.coalesce(F.col('h.max_dte'), F.col('a.max_dte'))) \
                         .select(F.col('keyword').alias('keyword'),
                                 F.when(F.col('a.min_dte') <= F.col('h_min'), F.col('a.min_dte')).otherwise(F.col('h_max')).alias('min_dte'),
                                 F.when(F.col('a.max_dte') >= F.col('h_max'), F.col('a.max_dte')).otherwise(F.col('h_min')).alias('max_dte')) \
                         .filter(F.col('min_dte') <= F.col('max_dte')).collect()
    return [r.asDict() for r in args]

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import udf
from typing import Iterator
import pandas as pd

import boto3
from botocore import UNSIGNED
from botocore.client import Config

@udf
def download_articles(accession_id: str,
                      articles_path: str,
                      file_type:str = "xml"):
  import boto3
  from botocore import UNSIGNED
  from botocore.client import Config  

  s3_conn = boto3.client('s3', config=Config(signature_version=UNSIGNED))
  try:
    s3_conn.download_file("pmc-oa-opendata",
                          f'oa_comm/{file_type}/all/{accession_id}.{file_type}',
                          f'{articles_path}/{accession_id}.{file_type}')
    return "DOWNLOADED"
  except:
    return "ERROR"

# COMMAND ----------

# This will return a list of needed pmids based upon what has already been searched and pulled as well as what is still pending download
# If keywords is left blank, range will be applied to all keywords aleady in search hist

from pyspark.sql import functions as F

def get_needed_pmids_df(search_hist: PubMedAsset,
                        metadata: PubMedAsset,
                        articles: PubMedAsset,
                        keywords: Union[str, List[str]] = None,
                        min_dte: str = "2022/01/01",
                        max_dte: Optional[str] = None):
    
    if keywords:
        keywords: [str] = keywords if isinstance(keywords, list) else [str(keywords),]
    else:
        keywords: [str] = [r.keyword for r in search_hist.df.select(F.col('keyword')).distinct().collect()]
    max_dte = max_dte or datetime.today().strftime('%Y/%m/%d')
    
    kwargs_list = get_search_hist_args(keywords, search_hist, min_dte, max_dte)

    pmids = searchPMCPapers(**kwargs_list[0])

    for kwargs in kwargs_list[1:]:
        sleep(0.5) #limit 2 api calls/sec
        pmids |= searchPMCPapers(**kwargs)

    pmids_df = metadata.df.sparkSession.createDataFrame([(i,) for i in pmids], "AccessionId STRING")
    file_type = metadata.name.split('_')[-1]
    articles_path = articles.path

    # NOTE: download_articles udf actually downloads the articles in addition to returning DOWNLOAD or ERROR status
    metadata_src = metadata.df.filter(F.col('Status') == F.lit('PENDING')) \
                           .join(pmids_df, "AccessionId", "leftsemi") \
                           .withColumn("Status",
                                       F.when(F.col('Retracted') == F.lit('yes'), F.lit("RETRACTED")) \
                                       .otherwise(download_articles(F.col("AccessionId"),
                                                                    F.lit(articles_path),
                                                                    F.lit(file_type)))).cache()

    # Update metadata table
    pubmed.raw_metadata.dt.alias("tgt").merge(source = metadata_src.alias("src"),
                                              condition = "src.AccessionID = tgt.AccessionID") \
        .whenMatchedUpdateAll() \
        .execute()
    # metadata_src will likwly be instpected after return, if unpersist is run, would rerun entire download which is not desired
    # metadata_src.unpersist()

    # Update search_hist
    pubmed.raw_search_hist.dt.alias("tgt").merge(source = spark.createDataFrame(kwargs_list).alias("src"),
                                                 condition = "src.keyword = tgt.keyword") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    # NOTE: Removal of RETRACTED Files will be moved into 01 process metadata (originally was part of download process in notebook 02)

    # TODO: Return a query that is only the insert records of the last run so that we don't run the risk of triggering another download
    return metadata_src

# COMMAND ----------


