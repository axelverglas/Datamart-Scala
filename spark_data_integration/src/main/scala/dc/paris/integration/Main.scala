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

  // Lecture d'un fichier Parquet local (ex: dans data/raw)
  val df = spark.read.parquet("../data/raw/yellow_tripdata_2024-12.parquet")

  // Écriture du fichier vers Minio dans le bucket "taxidata"
  df.write
    .mode("overwrite")
    .parquet("s3a://taxidata/yellow_tripdata_2024-12.parquet")

  println("✅ Upload terminé.")
  spark.stop()
}
