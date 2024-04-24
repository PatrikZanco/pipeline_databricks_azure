// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("mnt/dados/bronze")

// COMMAND ----------

// MAGIC %md
// MAGIC lendo dados da camada bronze

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/bronze_dataset_imoveis"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC transformando os campos json em colunas

// COMMAND ----------

display(df.select ("anuncio.*"))

// COMMAND ----------

val dados_detalhados = df.select ("anuncio.*", "anuncio.endereco.*")

display(dados_detalhados)
  

// COMMAND ----------

// MAGIC %md
// MAGIC removendo colunas

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")

// COMMAND ----------

display(df_silver)

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


