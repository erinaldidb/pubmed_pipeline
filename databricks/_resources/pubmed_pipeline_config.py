# Databricks notebook source
# reset_all_data is helpful during development since it provides an easy way to recreate all entities to test all application methods
# TODO: Implement clean-up of all assets. This isn't essential for production deployment, but is a convenience feature that can help train others.
dbutils.widgets.dropdown(name="FILE_TYPE",
                         defaultValue="xml",
                         choices=["xml", "txt"])
dbutils.widgets.dropdown(name="DISPLAY_CONFIGS",
                         defaultValue="false",
                         choices=["false", "true"])
dbutils.widgets.dropdown(name="RESET_ALL_DATA",
                         defaultValue="false",
                         choices=["false", "true"])

# COMMAND ----------

# Primary Configurations:
# These are Application Level Configurations that may change between environments, but should remain consistant between notebooks:
PUBMED_CATALOG = 'pubmed_pipeline'
PUBMED_SCHEMA_RAW = 'raw'
PUBMED_SCHEMA_CURATED = 'curated'
PUBMED_SCHEMA_PROCESSED = 'processed'

# NOTE: we could proceed with all naming conventions being static assigned like above, but instead we'll use a class structure to help automatically configure at first use. This will add negligable inspects as part of IF EXISTS syntax, but will remove need to maintain state verification as a separate pre-process to our job.

# COMMAND ----------

import pyspark
import delta
from dataclasses import dataclass, field
from pyspark.sql import SparkSession
from functools import cached_property
import re
import os

@dataclass
class PubMedAsset:
    # This is effectively a base class for UC entities. It will help standardize getting our configs from *.sql files.
    uc_name: str
    create_sql_file: str

    @cached_property
    def _spark(self) -> SparkSession:
        return SparkSession.builder.getOrCreate()

    @cached_property
    def create_sql_path(self) -> str:
        path = ["", "Workspace"] + dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[1:]
        if path[-2] == '_resources':
            return "/".join(path[:-1] + [self.create_sql_file,])
        elif path[-2] == 'databricks':
            return "/".join(path[:-1] + ["_resources", self.create_sql_file])

    @cached_property
    def create_sql_relative_url(self) -> str:
        return self.create_sql_path.replace("/Workspace/","#workspace/")

    @cached_property
    def create_sql(self) -> str:
        with open(self.create_sql_path, 'r') as f:
            return f.read()

    @cached_property
    def name(self) -> str:
        # First time PubMedTable.name is called create sql if exists is run
        sql = self.create_sql
        kwargs = {k:getattr(self, k) for k in set(self.__dir__()).intersection(set(re.findall(r"\{(.*?)\}", sql)))}
        self._spark.sql(sql.format(**kwargs))
        if hasattr(self,"_path_value"):
            # Means this is a volume and we need to instantiate the folders
            os.makedirs("/".join((['', 'Volumes',] + self.uc_name.split('.') + [self._path_value,])), exist_ok=True)
        return self.uc_name

# COMMAND ----------

@dataclass
class PubMedTable(PubMedAsset):
    # Our table class which uses our base class, every table in our application should get this class assigned

    @property
    def df(self) -> pyspark.sql.DataFrame:
        return self._spark.table(self.name)

    @property
    def dt(self) -> delta.tables.DeltaTable:
        return delta.tables.DeltaTable.forName(self._spark, self.name)
    
    @cached_property
    def uc_relative_path(self) -> str:
        path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")
        if path[-2] == '_resources':
            return '../../explore/data/' + '/'.join(self.name.split('.'))
        elif path[-2] == 'databricks':
            return '../explore/data/' + '/'.join(self.name.split('.'))

# COMMAND ----------

@dataclass
class PubMedVolume(PubMedAsset):
    # Our volume class which uses our base class, every volume in our application should get this class assigned
    _path_value: str = ""

    @cached_property
    def volume_root(self):
        vol_path_list = ['', 'Volumes',]
        vol_path_list += self.name.split('.')
        return '/'.join(vol_path_list)
    
    @cached_property
    def path(self):
        # Returns the complete path which is the ultimate value we will use in our application workflow
        if self._path_value == "" :
            return self.volume_root
        else:
            return self.volume_root + '/' + self._path_value
        
    @cached_property
    def uc_relative_path(self) -> str:
        # TODO: Add volume path
        path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")
        if path[-2] == '_resources':
            return '../../explore/data/volumes/' + '/'.join(self.name.split('.'))
        elif path[-2] == 'databricks':
            return '../explore/data/volumes/' + '/'.join(self.name.split('.'))

# COMMAND ----------

import pyspark
import delta
from dataclasses import dataclass, field
from pyspark.sql import SparkSession
from functools import cached_property
import re
import inspect


@dataclass
class PubMedConfig:
    # This is the class we'll use to conslidate our uc application entities into a single configuration
    catalog_name: str ='pubmed_pipeline'
    schema_raw_name: str = 'raw'
    schema_curated_name: str = 'curated'
    schema_processed_name: str  = 'processed'
    file_type: str = ''

    def __post_init__(self):
        if (self.file_type == "") and ("FILE_TYPE" in dbutils.notebook.entry_point.getCurrentBindings()):
            self.file_type = dbutils.notebook.entry_point.getCurrentBindings()['FILE_TYPE']
        setattr(self, 'raw_search_hist',
                    PubMedTable(uc_name = f'{self.catalog_name}.{self.schema_raw_name}.search_hist',
                                create_sql_file = 'CREATE_TABLE_raw_search_hist.sql'))
        setattr(self, 'processed_articles_content',
                    PubMedTable(uc_name = f'{self.catalog_name}.{self.schema_processed_name}.articles_content',
                                create_sql_file = 'CREATE_TABLE_processed_articles_content.sql'))
        # Only include tables & volumes that are dependent upon FILE_TYPE if exists in bindings
        if self.file_type != "":
            setattr(self, 'raw_metadata',
                    PubMedTable(uc_name = f'{self.catalog_name}.{self.schema_raw_name}.metadata_{self.file_type}',
                                create_sql_file = 'CREATE_TABLE_raw_metadata.sql'))
            setattr(self.raw_metadata, 'cp',
                    PubMedVolume(uc_name = f'{self.catalog_name}.{self.schema_raw_name}._checkpoints',
                                 create_sql_file = 'CREATE_VOLUME_raw_checkpoints.sql',
                                 _path_value=f'metadata_{self.file_type}'))
            setattr(self, 'raw_articles',
                    PubMedVolume(uc_name = f'{self.catalog_name}.{self.schema_raw_name}.articles',
                                 create_sql_file = 'CREATE_VOLUME_raw_articles.sql',
                                 _path_value=f'all/{self.file_type}'))
            setattr(self, 'curated_articles',
                    PubMedTable(uc_name = f'{self.catalog_name}.{self.schema_curated_name}.articles_{self.file_type}',
                                create_sql_file = 'CREATE_TABLE_curated_articles.sql'))
            setattr(self.curated_articles, 'cp',
                    PubMedVolume(uc_name = f'{self.catalog_name}.{self.schema_curated_name}._checkpoints',
                                 create_sql_file = 'CREATE_VOLUME_curated_checkpoints.sql',
                                 _path_value=f'articles_{self.file_type}'))
            setattr(self, 'processed_articles_content',
                    PubMedTable(uc_name = f'{self.catalog_name}.{self.schema_processed_name}.articles_content',
                                create_sql_file = 'CREATE_TABLE_processed_articles_content.sql'))
            setattr(self.processed_articles_content, 'cp',
                    PubMedVolume(uc_name = f'{self.catalog_name}.{self.schema_processed_name}._checkpoints',
                                 create_sql_file = 'CREATE_VOLUME_processed_checkpoints.sql',
                                 _path_value=f'articles_content_{self.file_type}'))            


    @cached_property
    def spark(self) -> SparkSession:
        return SparkSession.builder.getOrCreate()

# COMMAND ----------

pubmed = PubMedConfig(catalog_name = PUBMED_CATALOG,
                      schema_raw_name = PUBMED_SCHEMA_RAW,
                      schema_curated_name = PUBMED_SCHEMA_CURATED,
                      schema_processed_name = PUBMED_SCHEMA_PROCESSED)

# COMMAND ----------

inspect_assets = dbutils.widgets.getArgument("DISPLAY_CONFIGS")
if inspect_assets == 'true':
    inspect_html = f"""<table border="1" cellpadding="10">
    <tr><th style="background-color: orange;">pubmed Asset</th>
        <th style="background-color: orange;">Attributes</th>
        <th style="background-color: orange;">Description</th></tr>
    <tr><td ROWSPAN=3><b>raw_metadata</b></td>
        <td>ddl: <a href={pubmed.raw_metadata.create_sql_relative_url}>{pubmed.raw_metadata.create_sql_file}</a></td>
        <td ROWSPAN=3><b>raw_metadata</b> is the table that syncs with all PubMed articles list.</br>It will also maintain the download status of all articles.</td></tr>
    <tr><td>table: <a href={pubmed.raw_metadata.uc_relative_path}>{pubmed.raw_metadata.name}</a></td></tr>
    <tr><td>cp: <a href={pubmed.raw_metadata.cp.uc_relative_path}>{pubmed.raw_metadata.cp.name}</a></td></tr>
    <tr><td ROWSPAN=2><b>raw_search_hist</b></td>
        <td>ddl: <a href={pubmed.raw_search_hist.create_sql_relative_url}>{pubmed.raw_search_hist.create_sql_file}</a></td>
        <td ROWSPAN=2><b>raw_search_hist</b> Is where we will store previous searches used to avoid having larger search windows.</td></tr>
    <tr><td>table: <a href={pubmed.raw_search_hist.uc_relative_path}>{pubmed.raw_search_hist.name}</a></td></tr>
    <tr><td ROWSPAN=2><b>raw_articles</b></td>
        <td>ddl: <a href={pubmed.raw_articles.create_sql_relative_url}>{pubmed.raw_articles.create_sql_file}</a></td>
        <td ROWSPAN=2><b>raw_articles</b> is the table that syncs with all PubMed articles list.</br>It will also maintain the download status of all articles.</td></tr>
    <tr><td>data: <a href={pubmed.raw_articles.uc_relative_path}>{pubmed.raw_articles.name}</a></td></tr>    
    </table>
    TODO: Brad add remaining assets"""
    displayHTML(inspect_html)
