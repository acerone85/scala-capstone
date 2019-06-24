package observatory


import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import Visualization._

trait VisualizationTest extends FunSuite with Checkers {
    val LONDON = Location(51.5074, 0.1278)
    val ROME = Location(41.8919,12.5113)

    val colorList = List[(Temperature, Color)](
        (60, Color(255,255,255)),
        (32, Color(255,0,0)),
        (12, Color(255,255,0)),
        (0, Color(0,0,255)),
        (-27, Color(255,0,255)),
        (-50, Color(33, 0, 107)),
        (-60, Color(0, 0, 0))
    )

    test("Distance between London and Rome should be around 1435KM") {
        val distance = distanceFrom(LONDON)(ROME)
        assert(distance >= 1415 && distance <= 1455)
    }

    test("Temperature in Color list should be approximated to given color") {
        assert(interpolateColor(colorList, 32) === Color(255,0,0))
    }

    test("Greater temperature should be approximated to color of greatest temperature") {
        assert(interpolateColor(colorList, 90) === Color(255,255,255))
    }

    test("Lower temperature should be approximated to color of lowest temperature") {
        assert(interpolateColor(colorList, -90) === Color(0,0,0))
    }

    test("Empty list of Color is handled correctly") {
        assert(interpolateColor(List(), 0) === Color(0,0,0))
    }

    test("Color Interpolation is performed correctly") {
        val testTemperature = 46
        val resultingColor = interpolateColor(colorList, testTemperature)

        assert(resultingColor === Color(255, 128, 128))
    }



}
