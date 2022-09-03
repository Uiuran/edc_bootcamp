import pyspark

spark = pyspark.sql.SparkSession.builder.appName("ExercicioSpark").getOrCreate()

enem = spark.read.options(inferSchema=True,header=True,delimiter=";").csv("s3://datalake-igti-edc-penalvad/raw-data/MICRODADOS_ENEM_2020.csv")

enem.coalesce(1).write.mode("overwrite").format("parquet").save("s3://datalake-igti-edc-penalvad/staging")
