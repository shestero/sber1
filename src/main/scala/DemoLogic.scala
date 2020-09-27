import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions._

object DemoLogic extends Spark(DemoApp.appName) {

  case class RatingRecord(userId: Int,
                          itemId: Int,
                          rating: Int,
                          timestamp: Long)

  val inputFile = "u.data"

  val inputData = spark.read
    .option("sep", "\t")
    .schema(Encoders.product[RatingRecord].schema)
    .csv(inputFile)

  val grouped =
    inputData.groupBy("itemId", "rating").agg(count("*").alias("cnt"))

  def hist(name: String, source: DataFrame) = {
    // All possible ratings [1..5]. End range value is exclusive, so 6 means 5.
    val ratings = spark.range(1, 6).withColumnRenamed("id", "rating")

    assert(source.count() <= ratings.count()) // accept only one-item DataFrame!

    ratings
      .join(source, Seq("rating"), "left")
      .agg(collect_list(struct("rating", "cnt")).alias("extcnt"))
      .withColumn(
        name,
        expr("transform(array_sort(extcnt), v -> coalesce(v.cnt,0))")) // it's possible to use .na.fill(0) instead of coalesce
      .drop("extcnt")
  }

  val hist_all =
    hist("hist_all", grouped.groupBy("rating").agg(sum("cnt").alias("cnt")))

  def process(itemId: String) = { // returns JSON string
    val hist_film = hist("hist_film", grouped.where(col("itemId") === itemId))

    val resultDF = hist_film join hist_all

    val resultJson = resultDF.toJSON.collect().mkString("")

    // Reformat using Circe:
    import io.circe.Json, io.circe.parser._, io.circe.syntax._
    parse(resultJson)
      .getOrElse(Json.Null)
      .asJson
      .spaces2
  }

}
