package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {

    //val dx0 = d00 + point.x * (d10 - d00)
    //val dx0 = d00 * ( 1 - point.x) + d10 * point.x
    //val dx1 = d01 + point.x * (d11 - d01)
    //val dx1 = d01 * (1 - point.x) + d11 * point.x

    //val dxy = dx0 + point.y * (dx1 - dx0)
    //val dxy = dx0 * (1 - point.y) + dx1 * point.y

    //dxy

    d00 * ( 1.0 - point.x) * (1.0 - point.y) + d10 * point.x * (1.0 - point.y) +
    d01 * (1.0 - point.x) * point.y + d11 * point.x * point.y
  }

  def predictDeviation(grid: GridLocation => Temperature, location: Location): Temperature = {

    val lat = location.lat
    val lon = location.lon

    //lat ranges over (-90,90]

    //val top = lat.ceil.toInt  //will be at least -89
    //val bottom = if (top == -89) 90 else top - 1

    val top = if (lat.floor.toInt == -90) 90 else lat.floor.toInt
    val bottom = if (top == 90) -89 else top + 1


    //lon ranges over [-180, 180)

    val left = lon.floor.toInt
    val right = if (left == 179) -180 else left + 1


    val point = CellPoint(lon - left, lat - top)

    val TLtemperature = grid(GridLocation(top,left))
    val TRtemperature = grid(GridLocation(top,right))
    val BLtemperature = grid(GridLocation(bottom,left))
    val BRtemperature = grid(GridLocation(bottom,right))

    val finalResult = bilinearInterpolation(point, TLtemperature, BLtemperature, TRtemperature, BRtemperature)

    finalResult
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {

    /*def coordinates(index: Int) : (Int, Int) = (index/256, index % 256)

    def debugPixelLocationsForTile(tile: Tile) = {
      Interaction.getPixelLocationsForTile(tile)
      .zip((0 until 256 * 256) map coordinates)
      .map { case(loc, index) =>
        if(index._1 == 0 && index._2 == 61) {
          println("DEBUG: found location at ( lat = " + loc.lat + ", lon = " + loc.lon +")")
          debugLocation = loc
        }
        loc
      }
    }*/

    val pixelArray =
      Interaction.getPixelLocationsForTile(tile)
      .map(predictDeviation(grid, _))
      .map(Visualization.interpolateColor(colors, _))
      .map(Interaction.colorToPixel)
      .toArray

    Image(256,256,pixelArray)

  }

}
