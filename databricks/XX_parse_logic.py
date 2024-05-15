# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Evaluation of XML parsing
# MAGIC
# MAGIC One of the more complex tasks of ingesting xml is the requirement to identify all of the fields of interest in an article. While the XML files retrieved from PMC look to be well curated and consistent across articles, they are also very detailed leading to a complex schema. Below is an example of what the schema looks like for a single example file.
# MAGIC
# MAGIC If we look at the schema, we will see that there really are not that many first level columns. Upon close inspection you will see that some of those fields are not actually child entities, but rather attributes of the article. Those fields when rendered using the Databricks XML format are: `_article-type`, `_dtd-version`, `_xml:lang`, `_xmlns:mml`, `_xmlns:xlink`.
# MAGIC
# MAGIC If we were to consolidate those fields into single `attrs` columns we would be left with the following, less numerous number of curated columns:
# MAGIC  * `attrs` - the consolidation of the attributes at the artile entity level (and not a child element)
# MAGIC  * `front` - the front page of the publication which includes notable fields like the article abstract, contributors, and funding. 
# MAGIC  * `body` - the article body - this will be the primary **content** that we will want to use for both our FT dataset as well as our vector store index
# MAGIC  * `floats-group` - the collection of non-text elements in a article that are common in research documents like figures or tables
# MAGIC  * `back` - the ending page which includes both notes and refrences
# MAGIC  * `processing-meta` - this will include details about metadata necessary for programatically processing the article xml

# COMMAND ----------

xmlPath = "/Volumes/pubmed_pipeline/raw/articles/all/xml/PMC10000549.xml"
article_df = spark.read.option("rowTag", "article").format("xml").load(xmlPath)
article_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructField
fields = [f.name for f in article_df.schema]
fields

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Granularity Decision for Curated Data
# MAGIC
# MAGIC Our use of the differnt schemas (`raw`, `curated`, `processed`) is consistent with a [medallion architecture](https://www.databricks.com/glossary/medallion-architecture). In the `curated` or **Silver Layer**, we are not requires to parse all these fields yet. Rather, we have a couple design objectives that we are seeking to implement:
# MAGIC  * **Matched** - In this case, we are seeking to have our curated articles match our raw metadata which is conveniently a 1:1 match when we make each article row that matches a single article row in our `raw_metadata` table.
# MAGIC  * **Conformed** - Again in this case, we have a convenince of PMC curation that our articles are all written into a shared (although complex) schema. We should be clear that while these articles are mostly consistant, there will be deviations in schema related to changes in metadata and author article body structure. Our confirmed in this case will be the high level column identification. This objective is much more open to debate as an extreme opinion would be to identify all posible article schemas within the populate, create a unified (and very complex) schema that all could be ingested into. On the other extreme, we could keep just a single column with all xml assigned as string. We will opt for something closer to the later that will make use of the highest level entities and attributes while leaving the details of those in original unprocessed XML as string.
# MAGIC  * **Cleansed ("just-enough")** - Data clensing would be more along the lines of identifying and mitigating erroneous entries and duplicate handling. 
# MAGIC
# MAGIC A data engineer may look at the minimal transform propsed and wonder what is the benefit of this intermediate `curated` or **Silver** form. Specific to our case with processing PubMed data, the transform in the data architecure is trivial and could simply be added to any workload that would read from the raw xml. However, the format change from individual uncompressed xml file to delta compressed form would infact make reading from blob storage faster. But that is not all - since we will be using a distributed compute framework (specifically [spark](https://spark.apache.org/)), we are able to have larger, fewer files for the articles corpus which has a framework preformance improvement impact. That impact is cause by the fact that such distributed frameworks assign tasks to distributed workers at the initial read of a directory by assigning a task by file - a directory of small xml files is inefficient becuase each file of about ~150M is assigned a worker which can have multipe GBs of memory which is inefficient. Additionally, cloud blob ls or list status functionality is relatively slow and listing directories of many small files can be a deterrent from want to work directly from raw files.
# MAGIC
# MAGIC Thus, in conclusion, our primary benefit for our intermediate form in PubMed data procesing is less about conformity and cleansing and more about down stream development and workflow peformance.
# MAGIC
# MAGIC One way to see the performance impact would be to simply run a query that counts the number articles. This will be exacerbated when using a small cluster.

# COMMAND ----------

xmlFolderPath = "/Volumes/pubmed_pipeline/raw/articles/all/xml"
cnt = spark.read.option("rowTag", "article").format("xml").load(xmlFolderPath).count()
print(cnt)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Beautiful Soup
# MAGIC
# MAGIC For parsing XML, we will suffer from having too many options of libraries we could choose from. We've opted to use the library [Beautiful Soup](https://beautiful-soup-4.readthedocs.io/en/latest/) which is mature, popular, and well documented. It provides an easy way to navigate HTML and XML data structures programatically. We'll evaluate reading a file directly from Volumes and coerse it into the schema we identified above.

# COMMAND ----------

# MAGIC %pip install beautifulsoup4

# COMMAND ----------

# We actually want to work with a single file during the parsing discovery process
# Instead of retyping a lot, we'll just use the first file of our test set above:

with open(xmlPath, 'r') as file:
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
body = article.find('body')
floats_group = article.find('floats-group')
back = article.find('back')
processing_metadata = article.find('rocessing-meta')

# It is recommended that data engineers get familiarity with the schema that the pubmed article xmls are written in
# Inspect all of the groups above

# COMMAND ----------

# This will be the data that we want to include in the attrs column
article = soup.find('article')
attrs = article.attrs
attrs

# COMMAND ----------

# An interesting component of the article front is the abstract which we can inspect here:
abstract = front.find('abstract')
abstract

# COMMAND ----------

# Heres an example of navigating the body sections and retriving the section titles
body = article.find('body')
titles = [sec.find('title').contents[0] for sec in body.find_all('sec')]
titles

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create an xml parse function
# MAGIC
# MAGIC We've defined above our target transform schema. Now we will use `bs4` to write a function that will transform our data into that format. We'll be making continued use of the `contents` property in `bs4` which will return the 
# MAGIC
# MAGIC **TODO**: We are not capturing the attributes of each tag because they were empty in the few files inspected. This should be confirmed on the larger articles population to make sure we are not dropping data that may exist in attributes for a subset of articles.

# COMMAND ----------

def curate_xml_dict(xmlPath: str):
    with open(xmlPath, 'r') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'xml')
    article_detail_map = {'dtd-version': 'dtd_version',
                          'xml:lang': 'xml_lang',
                          'article-type': 'article_type',
                          'xmlns:mml': 'xmlns_mml',
                          'xmlns:xlink': 'xmlns_xlink'}
    article_dict = {'attrs': {article_detail_map[k]: str(v) for k, v in soup.find('article').attrs.items() if k in article_detail_map.keys()},
                    'front':               str(article.find('front')),
                    'body':                str(article.find('body')),
                    'floats_group':        str(article.find('floats-group')),
                    'back':                str(article.find('back')),
                    'processing_metadata': str(article.find('processing-meta'))}
    return article_dict

# COMMAND ----------

article_xml_dict = curate_xml_dict(xmlPath)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Convert article_xml_dict to spark dataframe
# MAGIC
# MAGIC We now have a function that can read a single xml file into our desired curated schema. Using our single example file, we can now read into spark inspect what coerced schema will be in spark and delta. We'll do this in a couple steps by creating a dataframe, saving to a table, and evaluating the CREATE TABLE statement of the table.

# COMMAND ----------

article_xml_df = spark.createDataFrame([article_xml_dict, ])
article_xml_df.createOrReplaceTempView('article_xml_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `pubmed-pipeline`.test.article_curated AS
# MAGIC SELECT * FROM article_xml_df;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can now use our delta lake metastore methods to inspect the create table statement. We can then reference the output to write our `CREATE_TABLE_curated_articles.sql` file.

# COMMAND ----------

create_sql = 'SHOW CREATE TABLE `pubmed-pipeline`.test.article_curated'
create_table_string = spark.sql(create_sql).collect()[0].createtab_stmt
print(create_table_string)
