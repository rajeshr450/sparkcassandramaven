package rajuri

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object second extends App {
  val logger = Logger.getLogger("org")
  logger.setLevel(Level.ERROR)

  val ut = new SparkUtility
  val spark: SparkSession = ut.getSparkSession
  val ls = Seq(2, 3)
  val rdd = spark.sparkContext.parallelize(ls,1)
  val rd = rdd.map(x => Emp(x))
  //  val rdd = spark.sparkContext.cassandraTable("test", "emp").select("emp_id")
  rd.deleteFromCassandra("test", "emp",keyColumns = SomeColumns("emp_id"))
  //  println("completed deleting the records")


  //  sc.parallelize(Seq(Key("1"))).deleteFromCassandra("test", "word_groups", keyColumns = SomeColumns("emp_id")
  spark.stop()
}
