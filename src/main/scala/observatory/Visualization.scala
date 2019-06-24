package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  val EARTH_RADIUS_IN_KM = 6371.088

  /** @param degrees The amplitude of an angle in degrees,
    * @return The amplitude of the same angle in radians
    */
  def toRadians(degrees: Double): Double = {
    degrees / 180 * math.Pi
  }

  /** @param location1 The geographic coordinates of the first location
    * @param location2 The geographic coordinates of the second location
    * @return The great-circle distance between the two locations
    */
  def distanceFrom(location1: Location)(location2: Location): Double = {
    def areAntipodes: Boolean = {
      def antipodeLatitude(lat: Double) = -lat

      //the antipode of the meridian at longitude 0 is both +180 and -180
      //thus we use two different functions according to the convention we use
      //for the definition of antipode meridian of 0
      def antipodeLongitudePlus(lon: Double) = if (lon <= 0) lon + 180 else lon - 180

      def antipodeLongitudeMinus(lon: Double) = if (lon < 0) lon + 180 else lon - 180

      //to check if a location is antipode to another, we need to take into
      //account that any of the two conventions above for the antipode of the 0 meridian
      //may be in use
      (location1.lat == antipodeLatitude(location2.lat)) &&
      (location1.lon == antipodeLongitudePlus(location2.lon) ||
      location1.lon == antipodeLongitudeMinus(location2.lon))
    }

    def deltaSigma = {
      val latitude1InRadians = toRadians(location1.lat)
      val latitude2InRadians = toRadians(location2.lat)

      val longitude1InRadians = toRadians(location1.lon)
      val longitude2InRadians = toRadians(location2.lon)

      val lonDiff = Math.abs(longitude2InRadians - longitude1InRadians)

      if (location1 == location2) 0.0
      else if (areAntipodes) Math.PI
      else {
        Math.acos(
          Math.sin(latitude1InRadians) * Math.sin(latitude2InRadians) +
          Math.cos(latitude1InRadians) * Math.cos(latitude2InRadians) * Math.cos(lonDiff)
        )
      }
    }

    EARTH_RADIUS_IN_KM * deltaSigma
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {

    val parTemperatures = temperatures.par
    val distanceTemperatures =
      parTemperatures map { case (loc, temp) => (distanceFrom(loc)(location), temp) }

    def minPair(p1: (Double, Temperature), p2: (Double, Temperature)) = {
      if (p1._1 < p2._1) p1 else p2
    }

    val closestStationWithTemperature = distanceTemperatures reduce minPair
    if (closestStationWithTemperature._1 <= 1) closestStationWithTemperature._2

    else {
      val weightedTemperatures = distanceTemperatures
      .map { case (dist, temp) => (1 / (dist * dist), temp / (dist * dist)) }
      .reduce { (pair1, pair2) => (pair1._1 + pair2._1, pair1._2 + pair2._2) }

      weightedTemperatures._2 / weightedTemperatures._1
    }

  }

  def lineFromPoints(x1: Temperature, x2: Temperature)(y1:Int, y2: Int)(x: Temperature) : Int = {
    if (x1 == x2) y1
    /*else {
      val m : Double = (y2 - y1) / (x2 - x1)
      val q = y1 - m * x1
      (m * x + q).round.toInt
    }*/
    else {
      val weight = (x - x1)/(x2-x1)

      (y1 * (1 - weight) + y2 * weight).round.toInt
    }

  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {

    //first check if value is in points
    val inPoints = points.find(_._1 == value)

    inPoints match {
      case Some((_, color)) => color
      case None =>
            val (lower, upper) = points.partition(_._1 < value)
            if (lower.isEmpty && upper.isEmpty) Color(0,0,0)
            else if (lower.isEmpty) upper.minBy(_._1)._2
            else if (upper.isEmpty) lower.maxBy(_._1)._2
            else {
              val glb = lower.maxBy(_._1)
              val lub = upper.minBy(_._1)

              val lineSegment: (Int, Int) => Temperature => Int =
                lineFromPoints(glb._1, lub._1)

              val red = lineSegment(glb._2.red, lub._2.red)(value)
              val green = lineSegment(glb._2.green, lub._2.green)(value)
              val blue = lineSegment(glb._2.blue, lub._2.blue)(value)

              Color(red,green,blue)
            }

    }

  }


  def locationToCoordinate(loc: Location) : (Int, Int) = {
    ((-loc.lat + 90).round.toInt, (loc.lon + 180).round.toInt)
  }

  def coordinateToLocation(x: Int, y: Int) = {
    Location(90 - x, y - 180)
  }

  def colorToPixel(color: Color) : Pixel = {
    Pixel(color.red, color.green, color.blue, 255)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {

    def getPixelColor(row: Int, column: Int) = {
      val locationAtPixel = coordinateToLocation(row, column)
      val temperatureAtPixel = predictTemperature(temperatures, locationAtPixel)
      val colorAtPixel = interpolateColor(colors, temperatureAtPixel)

      colorToPixel(colorAtPixel)
    }

    val pixelArray = new Array[Pixel](180 * 360)

    val coordinates = pixelArray.indices map (index => (index/360, index % 360))
    val filledArray = ((pixelArray zip coordinates).par map {
      case (_, coordinate) => getPixelColor(coordinate._1, coordinate._2)
    }).toArray

    Image(360,180, filledArray)
  }




  def main(args: Array[String]): Unit = {

    val stationsFile = "/stations.csv"
    val temperaturesFile = "/2005.csv"

    val LONDON = Location(51.5074, 0.1278)
    val ROME = Location(41.8919, 12.5113)
    val NORTHPOLE = Location(90, 0)
    val SOUTHPOLE = Location(-89, 0)

    val colorList = List[(Temperature, Color)](
      (60, Color(255,255,255)),
      (32, Color(255,0,0)),
      (12, Color(255,255,0)),
      (0, Color(0,0,255)),
      (-27, Color(255,0,255)),
      (-50, Color(33, 0, 107)),
      (-60, Color(0, 0, 0))
    )


    println("Distance from North to SouthPole: " + distanceFrom(SOUTHPOLE)(NORTHPOLE))

    val temperatureRecords = Extraction.spark.time(Extraction.locateTemperatures(2005, stationsFile, temperaturesFile))
    val averageTemperatures = Extraction.spark.time(Extraction.locationYearlyAverageRecords(temperatureRecords))

    //averageTemperatures.take(20) foreach println

    val temperatureAtLondon = Extraction.spark.time(predictTemperature(averageTemperatures, LONDON))
    val temperatureAtSouthPole = Extraction.spark.time(predictTemperature(averageTemperatures, SOUTHPOLE))
    val temperatureAtRome = Extraction.spark.time(predictTemperature(averageTemperatures, ROME))

    println()
    println("Predicted Temperature: " + temperatureAtLondon)
    println("Predicted Temperature: " + temperatureAtSouthPole)
    println("Predicted Temperature: " + temperatureAtRome)

    val temperatureImage = visualize(averageTemperatures, colorList)
    temperatureImage.output(new java.io.File("target/someImage.png"))

    println("Done")

  }

}

