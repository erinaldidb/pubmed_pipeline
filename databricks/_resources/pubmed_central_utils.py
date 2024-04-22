# Databricks notebook source
# This notebook is to consolidate larger, complex functions used to interact with pubmed that will be used in the extract job

# COMMAND ----------

import requests
import defusedxml.ElementTree as ET
from time import sleep

#database info and filters
#https://eutils.ncbi.nlm.nih.gov/entrez/eutils/einfo.fcgi?db=pmc

#Search Example
#https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&usehistory=y&term=hpv[kwd]

#Paper in XML format
#https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=PMC10854087

def searchPMCPapers(keyword: str, limit_results=10):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&usehistory=y"
    ret_max = 100
    date_range = "mindate=2022/01/01&maxdate=2024/04/15"

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
  
    if limit_results:
        list_pmid = list(pmids)[:limit_results]
    else:
        list_pmid = list(pmids)
    return list_pmid

def get_keyword_pmids(keyword_search: str, limit_results=-1):
    set_pmid = set()
    if("/Volumes/" in keyword_search):
        with open(keyword_search, 'r') as keywords:
            for keyword in keywords.readlines():
                pmids = searchPMCPapers(keyword.strip())
                set_pmid.update(pmids)
    else:
        pmids = searchPMCPapers(keyword_search.strip())
        set_pmid.update(pmids)
    return set_pmid if limit_results == -1 else set_pmid[:limit_results]
