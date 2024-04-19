# Databricks notebook source
from pyspark.sql.functions import current_date

# COMMAND ----------

def read_csv(path):
  df = spark.read.csv(path, header=True, inferSchema=True)
  return df

# COMMAND ----------

def write_csv(df, path):
  df.write.format('csv').save(path)


# COMMAND ----------

def read_with_custom_schema(data, schema):
    df = spark.read.csv(data, schema)
    return df


# COMMAND ----------

def read_with_custom_schema_format(data, schema):
    df = spark.read.format('csv').schema(schema).load(data)
    return df

# COMMAND ----------

# DBTITLE 1,camel to snake case
def camel_to_snake_case(df):
    for cols in df.columns:
        df = df.withColumnRenamed(cols, cols.lower())
    return df

# COMMAND ----------

def add_current_date(df):
    df = df.withColumn("load_date", current_date())
    return df