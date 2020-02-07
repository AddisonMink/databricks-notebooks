// Databricks notebook source
import org.apache.spark.sql.functions._

val file = "/FileStore/tables/county_population.csv"
val fileType = "csv"
val inferSchema = true
val firstRowIsHeader = true
val delimiter = ","

val countyDf = spark.read.format(fileType)
  .option("inferSchema", inferSchema)
  .option("header",firstRowIsHeader)
  .option("sep",delimiter)
  .load(file)
  .select($"state_name", $"county_name", $"pop2014")
  .filter($"county_name".contains("County"))

// COMMAND ----------

val highestPopDf = countyDf
  .groupBy("state_name")
  .agg(max("pop2014").alias("pop2014"))

val mostPopulousCountiesByState = countyDf.join(highestPopDf,Seq("state_name","pop2014"))
  .orderBy(desc("pop2014"))
  .select($"state_name".alias("State"), $"county_name".alias("Most_Populous_County"), $"pop2014".alias("2014_County_Population"))

mostPopulousCountiesByState.write.mode("overwrite").parquet("/tmp/MostPopulousCountiesByState")

// COMMAND ----------

import org.apache.spark.sql.Column

def top_5_counties(arr: Column): Column = {
  val sorted = sort_array(arr,false)
  val sliced = slice(sorted,1,5)
  explode(sliced)
}

val top5PopsDf = countyDf
  .groupBy("state_name")
  .agg(top_5_counties(collect_list("pop2014")).alias("pop2014"))

val top5MostPopulousCountiesByState = countyDf.join(top5PopsDf,Seq("state_name","pop2014"))
  .orderBy($"state_name",desc("pop2014"))
  .select($"state_name".alias("State"), $"county_name".alias("Most_Populous_County"), $"pop2014".alias("2014_County_Population"))

top5MostPopulousCountiesByState.write.mode("overwrite").parquet("/tmp/Top5MostPopulousCountiesByState")

// COMMAND ----------

display(sqlContext.read.parquet("/tmp/MostPopulousCountiesByState"))

// COMMAND ----------

display(sqlContext.read.parquet("/tmp/Top5MostPopulousCountiesByState"))
