package rajuri

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object third extends App{

  val logger = Logger.getLogger("org")
  logger.setLevel(Level.ERROR)

  val ut = new SparkUtility
  val spark: SparkSession = ut.getSparkSession


  val ls = Seq(2, 3)
  val rdd = spark.sparkContext.parallelize(ls,1)

  val ll = rdd.collect().toList


  val cdbconnector = CassandraConnector(spark.sparkContext.getConf)
  val session = cdbconnector.openSession()
  ll.foreach(ele => {
    val delete = s"DELETE FROM test.emp where emp_id=" + ele + ";"
    session.execute(delete)
  })
  session.close()

  spark.stop()

}
