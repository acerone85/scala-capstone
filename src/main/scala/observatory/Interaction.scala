package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import java.util.concurrent._


/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  val colorList = List[(Temperature, Color)](
    (60, Color(255,255,255)),
    (32, Color(255,0,0)),
    (12, Color(255,255,0)),
    (0, Color(0,0,255)),
    (-27, Color(255,0,255)),
    (-50, Color(33, 0, 107)),
    (-60, Color(0, 0, 0))
  )

  def closestIndexInCache(temp: Double) = {
    ((temp * 25).round * 4).toInt
  }

  val temperaturesCache = {
    var cache = Map.empty[Int, Color]

    val temperatureRange = -6000 to 6000 by 4

    for (temp <- temperatureRange) {
      val normalisedTemp = temp/100.toDouble
      cache = cache + (temp -> Visualization.interpolateColor(colorList,normalisedTemp))
    }
    //println("Cache for temperatures Successfully Computed")

    cache
  }

  def fastColorInterpolation(temp: Temperature) = {
    if (temp >= 60) Color(255,255,255)
    if (temp <= 60) Color(0,0,0)

    temperaturesCache(closestIndexInCache(temp))
  }

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val lat = math.atan(math.sinh(math.Pi - tile.y/math.pow(2,tile.zoom) * 2 * math.Pi)) * 180/math.Pi
    val lon = tile.x/math.pow(2, tile.zoom) * 360 - 180

    Location(lat, lon)
  }

  def getPixelLocationsForTile(tile: Tile) = {
    val emptyArray = new Array[Unit](256 * 256).par

    val tileArray = emptyArray.zipWithIndex map {case (_, index) => (index/256, index % 256)}

    val corner = (tile.x * 256, tile.y * 256)
    val zoomLevel = tile.zoom + 8

    tileArray
      .map {case (row,column) => Tile(corner._1 + column, corner._2 + row, zoomLevel)}
      .map(tileLocation)

    /*def createFillMatrixTask(tile: Tile, row: Int, column: Int, level: Int) : ForkJoinTask[Unit] = {
      val t = new RecursiveTask[Unit] {
        def compute = fillMatrix(tile, row, column, level)
      }

      forkJoinPool.execute(t)
      t
    }

    def fillMatrix(tile: Tile, row: Int, column: Int, remainingLevels: Int): Unit = {
      if (remainingLevels == 0) resultArray(256 * row + column) = tileLocation(tile)
      else {
        val newZoom = tile.zoom + 1
        val startX = tile.x * 2
        val startY = tile.y * 2

        val NWtile = Tile(startX, startY, newZoom)
        val NEtile = Tile(startX + 1, startY, newZoom)
        val SWtile = Tile(startX, startY + 1, newZoom)
        val SEtile = Tile(startX + 1, startY + 1, newZoom)

        val NWTask = createFillMatrixTask(NWtile, 2 * row, 2 * column, remainingLevels - 1)
        val NETask = createFillMatrixTask(NEtile, 2 * row, 2 * column + 1, remainingLevels - 1)
        val SWTask = createFillMatrixTask(SWtile, 2 + 1 * row, 2 * column, remainingLevels - 1)
        val SETask = createFillMatrixTask(SEtile, 2 * row + 1, 2 * column + 1, remainingLevels - 1)

        NWTask.join()
        NETask.join()
        SWTask.join()
        SETask.join()
      }
    }

    fillMatrix(tile, 0, 0, 8)
    resultArray */

  }

  def colorToPixel(color: Color) : Pixel = {
    Pixel(color.red, color.green, color.blue, 127)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val pixelArray =
      getPixelLocationsForTile(tile)
      .map(Visualization.predictTemperature(temperatures, _))
      .map(Visualization.interpolateColor(colors, _))
      .map(colorToPixel).toArray

    Image(256, 256, pixelArray)
  }

  def fastTile(temperatures: Iterable[(Location, Temperature)], tile: Tile): Image = {
    val pixelArray =
      getPixelLocationsForTile(tile)
      .map(Visualization.predictTemperature(temperatures, _))
      .map(fastColorInterpolation(_))
      .map(colorToPixel).toArray

    Image(256, 256, pixelArray)
  }

  def subTiles(tile: Tile, level: Int) = {
    val depth = math.pow(2, level).round.toInt
    val emptyArray = new Array[Unit](depth * depth).par

    val tileArray = emptyArray.zipWithIndex map {case (_, index) => (index/depth, index % depth)}

    val corner = (tile.x * depth, tile.y * depth)
    val zoomLevel = tile.zoom + level

    tileArray.map {case (row,column) => Tile(corner._1 + column, corner._2 + row, zoomLevel)}
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    yearlyData.par foreach { case (year, data) =>
      for (level <- 0 to 3) {
        subTiles(Tile(0, 0, 0), level) foreach (generateImage(year, _, data))
      }
    }
  }

  def temperatureDirName(year: Year, tile: Tile): String = {
    "target/temperatures/" + year + "/" + tile.zoom
  }

  def temperatureFilename(year: Year, tile: Tile): String = {

    temperatureDirName(year, tile) + "/" + tile.x + "-" + tile.y + ".png"
  }

  def generateImage(year: Year, tile: Tile, temperatures: Iterable[(Location, Temperature)]): Unit = {

    val filename = temperatureFilename(year, tile)

    val directory = temperatureDirName(year, tile)

    val dirPath = java.nio.file.Paths.get(directory)

    try {
      java.nio.file.Files.createDirectories(dirPath)
    }
    catch {
      case e : Exception => e.printStackTrace()
    }
    finally {
      val generatedImage: Image = fastTile(temperatures, tile)
      generatedImage.output(new java.io.File(filename))
    }

  }

  def main(args: Array[String]): Unit = {

    val stationsFile = "/stations.csv"

    val years = 1975 to 1975
    val temperatureFiles = years map ("/" + _ + ".csv")

    val temperatureRecords : Iterable[(Year, Iterable[(Location, Temperature)])] = Extraction.spark.time(
      years zip (years zip temperatureFiles)
      .map { case (year, file) => Extraction.locateTemperatures(year, stationsFile, file)}
      .map(Extraction.locationYearlyAverageRecords))

    Extraction.spark.time(generateTiles(temperatureRecords, generateImage))

  }

}
