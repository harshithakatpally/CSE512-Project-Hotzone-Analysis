package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
	
	val rectangle_coordinates = queryRectangle.split(",")
	val target_point_coordinates = pointString.split(",")

	val point_x: Double = target_point_coordinates(0).trim.toDouble
	val point_y: Double = target_point_coordinates(1).trim.toDouble
	val rect_x1: Double = math.min(rectangle_coordinates(0).trim.toDouble, rectangle_coordinates(2).trim.toDouble)
	val rect_y1: Double = math.min(rectangle_coordinates(1).trim.toDouble, rectangle_coordinates(3).trim.toDouble)
	val rect_x2: Double = math.max(rectangle_coordinates(0).trim.toDouble, rectangle_coordinates(2).trim.toDouble)
	val rect_y2: Double = math.max(rectangle_coordinates(1).trim.toDouble, rectangle_coordinates(3).trim.toDouble)

	if ((point_x >= rect_x1) && (point_x <= rect_x2) && (point_y >= rect_y1) && (point_y <= rect_y2)) {
		return true
	}
	return false
  }
}
