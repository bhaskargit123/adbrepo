# Databricks notebook source
data_table=  (
                spark.read.format("sqlserver")
                .option("host", "bhaskarsql.database.windows.net")
                .option("port", "1433")
                .option("user", "bhaskar")
                .option("password", "Password@123")
                .option("database", "DATASTORE")
                .option("dbtable", "dbo.employee")
                .load()
            )


# COMMAND ----------

data_table.show()
