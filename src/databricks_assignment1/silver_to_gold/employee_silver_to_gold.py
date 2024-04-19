# Databricks notebook source
# DBTITLE 1,import necessary modules
from pyspark.sql.functions import desc, count, avg

# COMMAND ----------

# DBTITLE 1,attach the utils notebook
# MAGIC %run "/Users/kesavan.k@diggibyte.com/databricks_assignment1/source_to_bronze/utils"

# COMMAND ----------

# MAGIC %run "/Users/kesavan.k@diggibyte.com/databricks_assignment1/bronze_to_silver/employee_bronze_to_silver"

# COMMAND ----------

# DBTITLE 1,read the table
employee_df = spark.read.format("delta").load('dbfs:/FileStore/assignments/questoin1/bronze_silver/employee_info/dim_employee')

# COMMAND ----------

# DBTITLE 1,find thes salary each deoartment in a desc oredr
salary_of_department = employee_with_date_df.orderBy(desc('salary'))

# COMMAND ----------

# DBTITLE 1,number of employees in each department located in each country
employee_count = employee_df.groupBy("department", "country").agg(count("employeeid").alias("employee_count"))

# COMMAND ----------

# DBTITLE 1,•	List the department names along with their corresponding country names
department_join_df = employee_with_date_df.join(department_with_date_df, employee_with_date_df.department == department_with_date_df.departmentid, "inner")
country_join_df = department_join_df.join(country_with_date_df, department_join_df.country == country_with_date_df.countrycode, "inner")
department_with_country = country_join_df.select('departmentname', 'countryname')

# COMMAND ----------

# DBTITLE 1,average age of employees in each department
avg_age_employee = employee_with_date_df.groupBy('department').agg(avg("age").alias('avg_age'))

# COMMAND ----------

# DBTITLE 1,overwrite and replace where condition on at_load_date.
employee_with_date_df.write.format("delta").mode("overwrite").option("replaceWhere", "load_date >= '2024-04-16'").save("/FileStore/assignments/gold/employee/table_name")

# COMMAND ----------

# DBTITLE 1,read the delta tables
test_df = spark.read.format('delta').load('dbfs:/FileStore/assignments/gold/employee/table_name/')

# COMMAND ----------

