import scala.util.matching.Regex
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.Encoders
import org.apache.spark.ml._

case class LogEvent(timestamp:String, etype:String, mesage:String)

object LogParser {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Logparser Streaming")
    val ssc = new StreamingContext(conf, Seconds(4))

    // define the regular expression - <date-time> <log-type> <message>
    val regex = raw"^([\d/]+ [\d:]+) ([a-zA-Z]+) (.*)".r

    // read only today's data
    val lines = ssc.textFileStream("/user/edureka_45354/temp/flume/events/" + java.time.LocalDate.now)
    
    // apply regex adn create a case calss from filter matches
    // val df = lines.map(regex.findAllIn(_)).filter(_.isEmpty == false).map(matches => LogEvent(matches.group(1), matches.group(2), matches.group(3)))
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
