package rajuri

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class SparkUtility {

  def getSparkSession:SparkSession ={

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("cassandra test")
      .config("spark.cassandra.connection.host","localhost")
      .config("spark.cassandra.connection.port","9042")
      .config("spark.cassandra.connection.ssl.enabled","false")
      .config("spark.cassandra.auth.username","cassandra")
      .config("spark.cassandra.auth.password","cassandra")
      .getOrCreate()
    spark

  }

}
