# Databricks notebook source
# DBTITLE 1,run the utils file in driver
# MAGIC %run "/Users/kesavan.k@diggibyte.com/databricks_assignment1/source_to_bronze/utils"

# COMMAND ----------

# DBTITLE 1,assign read path to variable
employee_read_path = 'dbfs:/FileStore/resources/Employee_Q1.csv'
department_read_path = 'dbfs:/FileStore/resources/Department_Q1.csv'
country_read_path = 'dbfs:/FileStore/resources/Country_Q1.csv'

# COMMAND ----------

# DBTITLE 1,read the csv
employee_df = read_csv(employee_read_path)
department_df = read_csv(department_read_path)
country_df = read_csv(country_read_path)

# COMMAND ----------

# DBTITLE 1,assign write path to variable
employee_write_path = 'dbfs:/FileStore/assignments/source_to_bronze/employee.csv'
department_write_path = 'dbfs:/FileStore/assignments/source_to_bronze/department.csv'
country_write_path = 'dbfs:/FileStore/assignments/source_to_bronze/country.csv'

# COMMAND ----------

# DBTITLE 1,write the csv file
write_csv(employee_df, employee_write_path)
write_csv(department_df, department_write_path)
write_csv(country_df, country_write_path)

# COMMAND ----------

