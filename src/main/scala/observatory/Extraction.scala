package observatory

import java.time.LocalDate

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext



/**
  * 1st milestone: data extraction
  */

object Extraction {

  val TemperatureType = DoubleType

   val STNID         = "STN identifier"
   val WBANID        = "WBAN identifier"
  val LATITUDE      = "Latitude"
  val LONGITUDE     = "Longitude"
  val MONTH         = "Month"
  val DAY           = "Day"
  val TEMPERATURE   = "Temperature"

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession =
  SparkSession
    .builder()
    .appName("Time Usage")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  def fsPath(resource: String) = {
    Paths.get(getClass.getResource(resource).toURI).toString
  }

  def toDoubleField(s: String): Option[Double] = {
    try { Some(s.toDouble) }
    catch { case e: Exception => None }
  }

  def toIntField(s: String): Option[Int] = {
    try { Some(s.toInt) }
    catch { case e: Exception => None }
  }

  def toCelsius(s: String): Option[Temperature] = {
    for { f <- toDoubleField(s) } yield (f - 32) * 5/9
  }


  def createStationsDataFrame(stationsFile: String) = {

    val stationSchema = {
      val stationId = StructField(STNID, StringType, false)
      val wbanId = StructField(WBANID, StringType, false)

      val latitude = StructField(LATITUDE, DoubleType, true)
      val longitude = StructField(LONGITUDE, DoubleType, true)

      StructType(Seq(stationId, wbanId, latitude, longitude))
    }

    def convertStationRow (line: List[String]): Row = {
      //assert(line.length == 4)
      val structuredLine: List[Any] = List(line(0), line(1),
        toDoubleField(line(2)).orNull, toDoubleField(line(3)).orNull)

      Row.fromSeq(structuredLine)
    }

    val fullyQualifiedPath = fsPath(stationsFile)
    val stationsRaw = sc.textFile(fullyQualifiedPath)
    //stationsRaw.take(20) foreach println

    //schema for stations: { STNid: String, WBANid: String, Latitude: String, Longitude: String }
    val splitRDD = stationsRaw map (_.split(",", 4).toList)
    //splitRDD take 20 foreach println

    val stationsRDD = splitRDD map convertStationRow


    spark.createDataFrame(stationsRDD, stationSchema).as("stations")
    .select(col("*"))
    .where((col(LATITUDE).isNotNull) and (col(LONGITUDE).isNotNull) )
    .cache()

  }

  def createTemperatureDataFrame(temperaturesFile: String) = {
    val temperatureSchema = {
      val stationId = StructField(STNID, StringType, false)
      val wbanId = StructField(WBANID, StringType, false)
      val month = StructField(MONTH, IntegerType, true)
      val day = StructField(DAY, IntegerType, true)
      val temperature = StructField(TEMPERATURE, TemperatureType, true)

      StructType(Seq(stationId, wbanId, month, day, temperature))
    }

    def convertTemperatureRow( line: List[String]): Row = {
      //assert(line.length == 5)
      val structuredLine: List[Any] = List(line(0), line(1),
        toIntField(line(2)).orNull, toIntField(line(3)).orNull, toCelsius(line(4)).orNull)

      Row.fromSeq(structuredLine)
    }

    val temperaturesRaw = sc.textFile(fsPath(temperaturesFile))
    val splitRDD = temperaturesRaw map (_.split(",", 5).toList)
    val temperaturesRDD = splitRDD map convertTemperatureRow

    spark.createDataFrame(temperaturesRDD, temperatureSchema).as("temperatures")
  }

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    def convertJoinedRow(year: Year)(row: Row): (LocalDate, Location, Temperature) = row match {
      case Row(_: String, _: String, latitude: Double, longitude: Double,
      month: Int, day: Int, temperature: Temperature) =>

        (LocalDate.of(year, month, day), Location(latitude, longitude), temperature)
    }

    val stationsDF = createStationsDataFrame(stationsFile)

    val temperaturesDF = createTemperatureDataFrame(temperaturesFile)

    val joinRDD = stationsDF.join(temperaturesDF,
      stationsDF(STNID) === temperaturesDF(STNID) and
      stationsDF(WBANID) === temperaturesDF(WBANID))

    val filteredJoinRDD = joinRDD.select(
      col("stations." + STNID) as STNID,
      col("stations." + WBANID) as WBANID,
      col("stations." + LATITUDE) as LATITUDE,
      col("stations." + LONGITUDE) as LONGITUDE,
      col("temperatures." + MONTH) as MONTH,
      col("temperatures." + DAY) as DAY,
      col("temperatures." + TEMPERATURE) as TEMPERATURE
    )


    /*implicit val LocalDateEncoder: Encoder[LocalDate] = Encoders.product[LocalDate]
    implicit val LocationEncoder: Encoder[Location] = Encoders.product[Location]
    implicit val TemperatureEncoder : Encoder[Temperature] = Encoders.scalaDouble
    implicit val TemperatureRecordEncoder: Encoder[(LocalDate, Location, Temperature)] =
      Encoders.tuple(LocalDateEncoder, LocationEncoder, TemperatureEncoder)

    val temperatureRecords = filteredJoinRDD map {
      row => row match {
        case (_: String, _: String, lat: Double, lon: Double, month: Int, day: Int, temp: Temperature) =>
          (LocalDate.of(year, month, day), Location(lat, lon), temp)
      }
    }

    import scala.collection.JavaConverters._
    temperatureRecords.toLocalIterator.asScala.toIterable*/

    filteredJoinRDD.collect() map convertJoinedRow(year)
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val recordsByLocation = (records groupBy (_._2)).par
    val structuredRecords = recordsByLocation mapValues
      {list => for {record <- list } yield (record._3, 1) }

    val aggregateRecords = structuredRecords mapValues (_ reduce {
      (cumulative, record) => (cumulative._1 + record._1, cumulative._2 + record._2)
    })

    val finalRecords = aggregateRecords mapValues { case (totalTemp, days) => totalTemp / days }

    finalRecords.toList
  }

  /*val tempRecords = locateTemperatures(1975, "/stations.csv", "/1975.csv")
  tempRecords.take(10) foreach println
  println("size: " + tempRecords.size)

  val averageRecords = locationYearlyAverageRecords(tempRecords)
  averageRecords.take(10) foreach println
  println("size: " + averageRecords.size)*/

}
