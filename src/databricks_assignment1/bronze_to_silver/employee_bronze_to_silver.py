# Databricks notebook source
# DBTITLE 1,run the utils notebook in bronze to silver
# MAGIC %run "/Users/kesavan.k@diggibyte.com/databricks_assignment1/source_to_bronze/utils"

# COMMAND ----------

# DBTITLE 1,import the necessary classes
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# DBTITLE 1,employee schema
employee_custom_schema = StructType([
    StructField('EmployeeID', IntegerType(), True),
    StructField('EmployeeName', StringType(), True),
    StructField('Department', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Salary', IntegerType(), True),
    StructField('Age', IntegerType(), True)
])

# COMMAND ----------

# DBTITLE 1,country schema
country_custom_schema = StructType([
    StructField('CountryCode', StringType(), True),
    StructField('CountryName', StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,department schema
department_custom_schema = StructType([
    StructField('DepartmentID', StringType(), True),
    StructField('DepartmentName', StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,store the file path as a variable
employee_csv_path = 'dbfs:/FileStore/resources/Employee_Q1.csv'
country_csv_path = 'dbfs:/FileStore/resources/Country_Q1.csv'
department_csv_path = 'dbfs:/FileStore/resources/Department_Q1.csv'

# COMMAND ----------

# DBTITLE 1,read the csv
employee_df = read_with_custom_schema(employee_csv_path,employee_custom_schema)
country_df =read_with_custom_schema(country_csv_path,country_custom_schema)
department_df = read_with_custom_schema(department_csv_path,department_custom_schema )

# COMMAND ----------

# DBTITLE 1,convert camel to snake case
employee_snake_case_df = camel_to_snake_case(employee_df)

# COMMAND ----------

department_snake_case_df = camel_to_snake_case(department_df)

# COMMAND ----------

country_snake_case_df = camel_to_snake_case(country_df)

# COMMAND ----------

# DBTITLE 1,add load_date column in employee table
employee_with_date_df = add_current_date(employee_snake_case_df)

# COMMAND ----------

# DBTITLE 1,add the load date column in department table
department_with_date_df = add_current_date(department_snake_case_df)

# COMMAND ----------

# DBTITLE 1,add the load date column in country table
country_with_date_df =add_current_date(country_snake_case_df)

# COMMAND ----------

# DBTITLE 1,create a database as employee_info
spark.sql('create database employee_info')

# COMMAND ----------

# DBTITLE 1,use the database
spark.sql('use employee_info')

# COMMAND ----------

# DBTITLE 1,write a table inside the database
employee_df.write.option('path', 'dbfs:/FileStore/assignments/questoin1/bronze_silver/employee_info/dim_employee').saveAsTable('dim_employee')