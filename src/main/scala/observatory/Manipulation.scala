package observatory

/**
  * 4th milestone: value-added information
  */
object Manipulation {

  type Grid = GridLocation => Temperature
  //type MutableMap[A,B] = scala.collection.mutable.Map[A,B]

  //def emptyMutableMap[A,B] = scala.collection.mutable.Map.empty[A,B]



  //var caches = emptyMutableMap[Iterable[(Location, Temperature)], MutableMap[GridLocation, Temperature]]

  var caches = Map.empty[Iterable[(Location, Temperature)], Array[Temperature]]

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {

    if(!(caches contains temperatures)) {
      val latRange = (90 until -90 by -1).par
      val lonRange = (-180 until 180).par
      val array = new Array[Temperature](180 * 360)

      for (lat <- latRange; lon <- lonRange) {
        //val gLoc = GridLocation(lat, lon)
        val loc = Location(lat, lon)

        val index = (90 - lat) * 360 + (lon + 180)
        array(index) = Visualization.predictTemperature(temperatures, loc)
      }

      caches = caches + (temperatures -> array)
    }

    def result(gLoc: GridLocation) : Temperature = {
      val array = caches(temperatures)
      array((90 - gLoc.lat) * 360 + (gLoc.lon + 180))
    }

    result
    /*if (!(caches contains temperatures)) {
      caches(temperatures) = emptyMutableMap[GridLocation, Temperature]
    }

    def temperatureAtGridLocation(gridLocation: GridLocation): Temperature = {
      val cache = caches(temperatures)

      if (cache contains gridLocation)
        cache(gridLocation)

      else {
        val location = Location(gridLocation.lat, gridLocation.lon)
        val computedTemperature = Visualization.predictTemperature(temperatures, location)

        cache += (gridLocation -> computedTemperature)

        computedTemperature
      }
    }

    temperatureAtGridLocation */
  }


  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val gridsPerYear = temperaturess map makeGrid
    val numberOfMeasurements = temperaturess.size

    if (numberOfMeasurements == 0) { _: GridLocation => 0 }

    else {
      def averageTemperature(gLoc: GridLocation): Temperature = {
        val temperaturesAtLocation = gridsPerYear map { grid: Grid => grid(gLoc) }
        val sumOfTemperatures = temperaturesAtLocation.sum

        sumOfTemperatures / numberOfMeasurements

      }

      averageTemperature
    }
  }



  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val yearlyGrid = makeGrid(temperatures)

    { gridLocation => yearlyGrid(gridLocation) - normals(gridLocation)}
  }


  def main(args: Array[String]) = {
    val yearsRange = 1975 to 1990
    val stationsFile = "/stations.csv"

    val testYear = 2015

    val yearInfo: Iterable[(Year, String)] = yearsRange zip (yearsRange map { year => "/" + year + ".csv" })

    def getYearlyAverageFromFile(year: Year, file: String) = {
      val yearlyRecords = Extraction.locateTemperatures(year, stationsFile, file)
      Extraction.locationYearlyAverageRecords(yearlyRecords)
    }

    val averageTemperaturesYearInfo: Iterable[Iterable[(Location, Temperature)]] = yearInfo
      .map {case (year, file) => getYearlyAverageFromFile(year, file) }

    val averageOfAverages = averageTemperaturesYearInfo map makeGrid

    val LONDONGRIDLOCATION = GridLocation(51, 0)

    averageOfAverages map { grid => grid(LONDONGRIDLOCATION)} foreach println

    println("Preparing function for averaging temperatures to 1990")
    val averageTemperaturesTo1990 = average(averageTemperaturesYearInfo)
    println("Done")

    println("getting yearly average temperatures in 2015")
    val averageTemperaturesIn2015 = getYearlyAverageFromFile(2015, "/2015.csv")
    println("Done")

    println("Preparing function for deviations in year 2015")
    val deviationsIn2015 = deviation(averageTemperaturesIn2015, averageTemperaturesTo1990)
    println("Done")

    Extraction.spark.time(println("Deviation of Temperature in London: " + deviationsIn2015(LONDONGRIDLOCATION)))
    Extraction.spark.time(println("Deviation of Temperature in London: " + deviationsIn2015(LONDONGRIDLOCATION)))
  }


}

