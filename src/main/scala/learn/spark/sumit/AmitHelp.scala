package learn.spark.sumit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.JsonMethods.parse

object AmitHelp {

  implicit val jsonFormats: Formats = DefaultFormats
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Json Reader")
      .config("spark.driver.memory", "3g")
      .getOrCreate()


    val df = spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .option("inferSchema", "true")
      .load("/Users/pawarsumit/Downloads/Book.xlsx")

    import spark.implicits._
    df
      .select(col("ALERT_ID"), col("FEATURE_COMPONENT_BREAKDOWN")).as[Det]
      .flatMap( det => {

        val comInfos = parse(det.FEATURE_COMPONENT_BREAKDOWN).extract[List[Comp]]
        comInfos.flatMap(comInfo => {
          comInfo.channelInfo.map(chnl =>
            Det2(
              det.ALERT_ID,
              comInfo.componentName,
              comInfo.value,
              chnl.channelType,
              chnl.value
            )
          )
        })

      }
      ).coalesce(1)
      .write.option("header", "true").csv("mydata.csv")


  }
}

case class Det(ALERT_ID: String, FEATURE_COMPONENT_BREAKDOWN: String)
case class Det2(ALERT_ID: String, componentName: String = "", componentValue:String ="", channelType: String ="", channelValue: String = "")

case class Comp(
                 componentName: String,
                 value: String,
                 channelInfo: List[ChannelInfo]
               )

case class ChannelInfo(
                        channelType: String,
                        value: String
                      )