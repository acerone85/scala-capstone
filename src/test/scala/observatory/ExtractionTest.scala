package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

trait ExtractionTest extends FunSuite {
    val stationFile = "/stations.csv"
    val year1975File = "/1975.csv"

    val stationDF = Extraction.createStationsDataFrame(stationFile).cache()
    val year1975DF = Extraction.createTemperatureDataFrame(year1975File)


    /*test("File contains 29444 stations") {
        assert(stationDF.count() === 29444)
    }*/

    /*test("First 5 records of stationRDD are as expected") {

        val actualRow : Array[Row] = stationDF.take(5)

        val expectedRow = new Array[Row](5)
        expectedRow(0) = Row(7005, null, null, null)
        expectedRow(1) = Row(7011, null, null, null)
        expectedRow(2) = Row(7018, null, 0.0,0.0)
        expectedRow(3) = Row(7025, null, null, null)
        expectedRow(4) = Row(7026, null, 0.0, 0.0)

        for (i <- expectedRow.indices)
            assert(actualRow(i) === expectedRow(i))
    }*/

    test("There are no 2 different stations with same (STNID, WBANID") {
        val groupedStationsDF = stationDF.groupBy(Extraction.STNID, Extraction.WBANID)
            .agg(count("*") as("cnt"))

        groupedStationsDF.show()

        val stationsPerIdDF = groupedStationsDF.filter(col("cnt") > 1)
        assert (stationsPerIdDF.collect().length === 0)
    }

    test("First 5 records for year 1975 are as expected") {
        //010010,,01,01,23.2
        //010010,,01,02,18.7
        //010010,,01,03,14.2
        //010010,,01,04,14.8
        //010010,,01,05,14.9
        val actualRow : Array[Row] = year1975DF.take(5)

        val expectedRow = new Array[Row](5)
        expectedRow(0) = Row("010010", "", 1, 1, Extraction.toCelsius("23.2").orNull)
        expectedRow(1) = Row("010010", "", 1, 2, Extraction.toCelsius("18.7").orNull)
        expectedRow(2) = Row("010010", "", 1, 3, Extraction.toCelsius("14.2").orNull)
        expectedRow(3) = Row("010010", "", 1, 4, Extraction.toCelsius("14.8").orNull)
        expectedRow(4) = Row("010010", "", 1, 5, Extraction.toCelsius("14.9").orNull)

        for (i <- expectedRow.indices)
            assert(actualRow(i) === expectedRow(i))
    }

}