package learn.spark.sumit
import org.apache.spark.sql._
case class  Users(name: String, age: Int, org: String, country: String)
object CassandraConnector {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Cassandra Connector")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val users = spark.read.option("header", "true")
      .csv("/Users/pawarsumit/software/spark-2.4.5-bin-hadoop2.7/data/users.csv")
      .withColumn("age", col("age").cast("int"))
      .as[Users]


/*
    users.write.mode(SaveMode.Append)
      .cassandraFormat("person", "users")
      .save()*/
  }

  def vargargs(a: String*): Iterator[String] = {
        new Iterator[String] {
          override def hasNext: Boolean = ???

          override def next(): String = ???
        }
  }

}
case class TTT(a: String)
