# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC To provide some context to what we are doing when we parse and chunk our xml files, we are going to inspect one locally. After we have the local functions that we'll want to use for parsing the docs, we'll go ahead and parallelize in a process that will populate the delta table that feeds our vector index.

# COMMAND ----------

# MAGIC %pip install mlflow==2.9.0 lxml==4.9.3 transformers==4.30.2 langchain==0.0.344 databricks-vectorsearch==0.22 bs4
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# When we are ready to process each of the files, they will actually be read from raw_metadata
# When testing we'll mock that source by just using some example xmls we've already downloaded
vol_paths = ['/Volumes/pubmed-pipeline/raw/articles/all/xml/PMC10000325.xml',
             '/Volumes/pubmed-pipeline/raw/articles/all/xml/PMC10000333.xml']
vols = spark.createDataFrame([(i,) for i in vol_paths], "volume_path STRING")
display(vols)

# COMMAND ----------

# We actually want to work with a single file during the parsing discovery process
# Instead of retyping a lot, we'll just use the first file of our test set above:
with open(vol_paths[0], 'r') as file:
    file_content = file.read()

# The output will be a little messy, but it's definately XML
# We'll use a simple python library for the xml parsing, specifically beautiful soup
print(file_content)

# COMMAND ----------

from bs4 import BeautifulSoup

# Reading file_content into a BeautifulSoup class will add the parsing methods that we'll use to inspect tags of interest
# Note that when we want to select a single first tag we use find and when we want to select multiple we use find_all
soup = BeautifulSoup(file_content, 'xml')
article = soup.find('article')
front = article.find('front')
abstract = front.find_all('abstract')
back = article.find('back')
body = article.find('body')
body_titles = body.find_all('title')
body_figures = body.find_all('figures')

# It is recommended that data engineers get familiarity with the schema that the pubmed article xmls are written in
# Inspect all of the groups above

# COMMAND ----------

# Front is consistantly in the article, we should consider which tags are of interest in front, one candidate is abstract
front

# COMMAND ----------

# This abstract is located within front - this type of nesting is pretty common
# abstract = front.find_all('abstract')
abstract

# COMMAND ----------

back.find_all('sec')[0].find('title')

# COMMAND ----------

# back itself has an interesting group of sections, one that might be of interest is key points
[s for s in back.find_all('sec') if s.find('title').contents[0] == 'Key Points']

# COMMAND ----------

# body is the section that we'll be most intertested in chunking
body
