# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT Status, Count(*) cnt FROM pubmed_pipeline.raw.metadata_xml GROUP BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Previously, when running ingest we read the files directly from S3 and wrote to delta. This appraoch was convenient because it allowed us to get the raw data into a high performance format, [delta](https://docs.databricks.com/en/delta/index.html), quickly. However, we should now take the time to look at the schema of our raw data and define the relavant fields that should be extracted.
# MAGIC
# MAGIC In this exercise, we will look at how to navigate xml file structures and come up with the fields of interest. That is what we will do below, we'll inspect the structure using the built xml format.
# MAGIC
# MAGIC ### The problem of small files
# MAGIC
# MAGIC In this working session, 25 APR 2024, we have downloaded sufficient records, , to highlight this problem. While there are many things that contribute to performance issues when working with small files, the essence is as follows:
# MAGIC
# MAGIC  * **Folders As Table**: Spark evolved with the Hadoop Distributed File System, HDFS. In this framework, files that had similar file structures were placed in a single folder. This convention made it simple to have a schemas as metadata saved in a metadata service where the most popular adopted version became [Apache Hive](https://hive.apache.org/). In fact, Databricks actually uses hive for the workspace metadata store and when viewed in unity catalog gets the name `hive_metastore`. Databricks unity catalog adopts many of these framework conventions today although the breadth of the metadata service now exceeds hive to include metadata management for things like models and volumes. 
# MAGIC
# MAGIC  * **Distributed Compute Frameworks**: Folders as tables was very effective for processing extremely data volumes and allowed companies to be able to process petabytes of data at the same time when using a distributed compute framework like [MapReduce](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#MapReduce_-_User_Interfaces) (now distributed with Hadoop), [Pig](https://pig.apache.org/) (effectively in maintenance mode), and [Spark](https://spark.apache.org/) . Each of these had a convention that a file within a folder was processed as a task and tasks were assigned to worker nodes with typically a task assigned to each cpu in a worker. Thus a cluster with 5 workers and 4 cpus each could process `4 X 5 = 20` tasks at a time.
# MAGIC
# MAGIC  * **The small files problem**: While we are able to put a schema on our metadata_xml raw file directory and read as a table it is increadibly inefficient. This is because each of our files are extremely small (\~150 KB) relative to the memory avaialable in our workers (\~14GB). With 4 cpus, that means that each task is effectively using `(4*150*1000) / (14*1000*1000*1000) = 0.00428%` of available resources. This inefficiency means that we'd have to increase the size of the cluster to get the desired performance benefits which doesn't make sense. We instead will want to reformat our raw data so that we have many more records saved in a single in a file which is accomplished when we save to delta.
# MAGIC
# MAGIC  * **The heavily nested problem**: Recieving articles in an xml format is very convenient since these articles can have aml hierachies that are very nested. However, this nesting is computationally expensive to traverse in a text file format. It can even be expensive to traverse in compute optimized file formats like [Hudi](https://hudi.apache.org/), [Iceberg](https://iceberg.apache.org/), and [Delta](https://delta.io/). Thus, when we write our files to delta, we are going to want to do some explosions. Enough that there isn't a single column for each file, but not so many that that there is a column for each possible value in the nested heirachy.
# MAGIC
# MAGIC Our exercise, will be to evaluate a single file which will be representative of other article xml files and come up with a schema we would like to have for our curated representation, `curated_articles`, of our raw files, `raw_articles`.
# MAGIC
# MAGIC This discovery will be done with the following reference code focusing on a single file, "/Volumes/pubmed_pipeline/raw/articles/all/xml/PMC10688988.xml".
# MAGIC
# MAGIC **NOTE**: Once we have this schema well defined, we will write a job that batch processes dozens or hundreds of files for a given task and will likely be implemented as a streaming job (undecided as of 15 APR 2024).
# MAGIC
# MAGIC **REF**: You can take a look at the complete list of supporte field types in spark in the [docs](https://spark.apache.org/docs/latest/sql-ref-datatypes.html).

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n5 /Volumes/pubmed_pipeline/raw/articles/all/xml/PMC10688988.xml

# COMMAND ----------

# MAGIC %sh
# MAGIC tail -n5 /Volumes/pubmed_pipeline/raw/articles/all/xml/PMC10688988.xml

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /Volumes/pubmed_pipeline/raw/articles/all/xml/PMC10688988.xml

# COMMAND ----------

df = spark.read.format('xml').options(rowTag='sec').load("/Volumes/pubmed_pipeline/raw/articles/all/xml/PMC10688988.xml")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can use the schema above to write our transforms. An example to include a field would be something like:

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.read.format('xml').options(rowTag='article').load("/Volumes/pubmed_pipeline/raw/articles/all/xml/PMC10688988.xml")
dat = df.select(F.col("_article-type"),
                F.col("_dtd-version"))
display(dat)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **NOTE**: This can also be done in [SQL](https://docs.databricks.com/en/query/formats/xml.html#sql-api) if the syntax is prefered over the pyspark API. However, this is less common and requires the use of built in sql functions.

# COMMAND ----------


