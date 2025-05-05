package dc.paris.integration

import org.apache.spark.sql.SparkSession

//import java.io.File



object Main extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Data Integration")
    .master("local[*]")
    .config("fs.s3a.access.key", "Kki2nyH5L1yO4uBBot9I") // A renseigner
    .config("fs.s3a.secret.key", "xtZDQ3UmiakqioOBbe4IIfdTXW15dWcwyvSBPfKh") // A renseigner
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "1000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()

  // 📥 Lecture depuis Minio (fichier .parquet)
  val df = spark.read.parquet("s3a://taxidata/yellow_tripdata_2024-10.parquet")

  println("✅ Lecture depuis Minio réussie.")
  df.printSchema()
  df.show(5)

  // 💾 Insertion dans PostgreSQL
  df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:15432/datawarehouse")
    .option("dbtable", "yellow_tripdata")
    .option("user", "postgres")
    .option("password", "admin")
    .mode("overwrite") // ou "overwrite" si tu veux remplacer à chaque fois
    .save()

  println("✅ Données insérées dans PostgreSQL.")
  spark.stop()
}
