# Databricks notebook source
# reset_all_data is helpful during development since it provides an easy way to recreate all entities to test all application methods
# TODO: Implement clean-up of all assets. This isn't essential for production deployment, but is a convenience feature that can help train others.
dbutils.widgets.dropdown(name="reset_all_data",
                         defaultValue="false",
                         choices=["false", "true"])
dbutils.widgets.dropdown(name="FILE_TYPE",
                         defaultValue="xml",
                         choices=["xml", "txt"])

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
    def create_sql(self) -> str:
        with open(self.create_sql_path, 'r') as f:
            return f.read()

    @cached_property
    def name(self) -> str:
        # First time PubMedTable.name is called create sql if exists is run
        sql = self.create_sql
        kwargs = {k:getattr(self, k) for k in set(self.__dir__()).intersection(set(re.findall(r"\{(.*?)\}", sql)))}
        self._spark.sql(sql.format(**kwargs))
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

# COMMAND ----------

@dataclass
class PubMedVolume(PubMedAsset):
    # Our volume class which uses our base class, every volume in our application should get this class assigned
    _path_value: str = ""

    @cached_property
    def volume_root(self):
        vol_path_list = ['', 'Volumes',]
        vol_path_list += self.uc_name.split('.')
        return '/'.join(vol_path_list)
    
    @cached_property
    def path(self):
        # Returns the complete path which is the ultimate value we will use in our application workflow
        if self._path_value == "" :
            return self.volume_root
        else:
            return self.volume_root + '/' + self._path_value

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
        tbls = {'processed_articles_content': {
                    '_tbl_name': f'{self.catalog_name}.{self.schema_processed_name}.articles_content',
                    'create_sql_file': 'CREATE_TABLE_processed_articles_content.sql'}}
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
                                 _path_value=f'articles_{self.file_type}'))
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

# MAGIC %md
# MAGIC
# MAGIC ### Naming Convention of `pubmed_pipeline_config`
# MAGIC
# MAGIC | pubmed_pipeline_config name     | description |
# MAGIC | ------------------------------- | ----------- |
# MAGIC | `raw_metadata`                  | A UC delta table that will maintain sync of PubMed Central index of articles.                                                 |
# MAGIC | `raw_metadata.cp`               | A UC volume path to `raw_metadata` checkpoints                                                                                |
# MAGIC | `raw_articles.data`             | A UC volume that will include the target location of a raw files downloaded from PubMed Central                               |
# MAGIC | `raw_articles.view`             | TODO: A UC view of data in `raw_articles` volume which is helpful for data inspection, quality assurance, and quick debugging |
# MAGIC | `curated_articles`              | A UC delta table that will maintain a "curated" delta table version of `raw_article` data                                     |
# MAGIC | `curated_articles.cp`           | A UC volume path to `curated_articles` checkpoints                                                                            |
# MAGIC | `processed_articles_content`    | A UC delta table that will maintain a "curated" delta table version of `raw_article` data                                     |
# MAGIC | `processed_articles_content.cp` | A UC volume path to `curated_articles` checkpoints                                                                            |

# COMMAND ----------

display_configs = True
if display_configs:
    print("pubmed.raw_metadata.uc_name: " + pubmed.raw_metadata.uc_name)
    print("pubmed.raw_metadata.cp.path: " + pubmed.raw_metadata.cp.path)
    print("pubmed.raw_articles.path: " + pubmed.raw_articles.path)
    print("pubmed.curated_articles.uc_name: " + pubmed.curated_articles.uc_name)
    print("pubmed.curated.cp.path: " + pubmed.curated_articles.cp.path)
    # NOTICE that processed_articles_content.uc_name has no file_type indicator in name, this is where we consolidate disparate file types to single table
    print("pubmed.processed_articles_content.uc_name: " + pubmed.processed_articles_content.uc_name)
    print("pubmed.processed_articles_content.cp.path: " + pubmed.processed_articles_content.cp.path) 
