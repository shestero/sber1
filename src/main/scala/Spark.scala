import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Spark(app: String, master: String = "local[*]") {

  // Spark:
  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(app)

  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")

  // For access to [local] file system
  val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set(
    "fs.hdfs.impl",
    classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  hadoopConfig.set("fs.file.impl",
                   classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

  println(s"Using Spark version=${sc.version}")

}
