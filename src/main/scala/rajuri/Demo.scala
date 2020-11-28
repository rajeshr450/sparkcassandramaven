package rajuri

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector


object Demo extends App {

  val logger = Logger.getLogger("org")
  logger.setLevel(Level.ERROR)

  val ut = new SparkUtility
  val spark: SparkSession = ut.getSparkSession


//  val readBooksDF = spark
//    .read
//    .format("org.apache.spark.sql.cassandra")
//    .options(Map("table" -> "emp", "keyspace" -> "test"))
//    .load
//  readBooksDF.show

  val joinWithRDD = spark.sparkContext.parallelize(0 to 5)
    .filter(_%2==0)
    .map(CustomerID(_))
//    .joinWithCassandraTable("test","customer_info")

//  val rr = joinWithRDD.map(x => x._2)
//  rr.deleteFromCassandra("test","customer_info")
  joinWithRDD.deleteFromCassandra("test","customer_info",keyColumns = SomeColumns("cust_id"))

//  rr.foreach(println)
  spark.stop()
}
