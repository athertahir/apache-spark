package training

import org.apache.spark.sql.SparkSession

object users {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Users")
      .master("local[*]")
      .getOrCreate()

    val users = spark.read
      .format("json")
      .options(Map("inferSchema" -> "true", "header" -> "false", "multiLine" -> "true"))
      .load("chapter_7/example_1.json")

    users.printSchema()

    users.show()
  }

}
