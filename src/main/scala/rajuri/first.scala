package rajuri

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector



object first extends App {
  val logger = Logger.getLogger("org")
  logger.setLevel(Level.ERROR)

  val ut = new SparkUtility
  val spark: SparkSession = ut.getSparkSession


  val ls = Seq(2, 3)
  val rdd = spark.sparkContext.parallelize(ls,1)
  import spark.implicits._
  val df = rdd.toDF("emp_id")


//  val session = cdbconnector.openSession()
  val cdbconnector = CassandraConnector(spark.sparkContext.getConf)
  df.foreachPartition(par => {
    cdbconnector.withSessionDo(session =>
    par.foreach(ele => {
      val delete = s"DELETE FROM test.emp where emp_id=" + ele + ";"
      session.execute(delete)
    })
    )
//    session.close()
  })


  df.printSchema()
  df.show()
  println("completed deleting records")
  spark.stop()
}
