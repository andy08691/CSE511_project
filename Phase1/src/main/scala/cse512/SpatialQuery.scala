package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {

    // Extract the coordinates of queryRectangle. The rectangle is defined by two points.
    var Rectangle = queryRectangle.split(",")
    val Rect_Point1_X = Rectangle(0).toDouble
    val Rect_Point1_Y = Rectangle(1).toDouble
    val Rect_Point2_X = Rectangle(2).toDouble
    val Rect_Point2_Y = Rectangle(3).toDouble

    // Extract the coordinates of pointString
    var Point = pointString.split(",")
    val Point_X = Point(0).toDouble
    val Point_Y = Point(1).toDouble

    // Determine the Min and Max rectangle coordinates
    var Min_X : Double = 0
    var Max_X : Double = 0
    var Min_Y : Double = 0
    var Max_Y : Double = 0

    if (Rect_Point1_X < Rect_Point2_X) {
      Min_X = Rect_Point1_X
      Max_X = Rect_Point2_X
    }
    else {
      Min_X = Rect_Point2_X
      Max_X = Rect_Point1_X
    }

    if (Rect_Point1_Y < Rect_Point2_Y) {
      Min_Y = Rect_Point1_Y
      Max_Y = Rect_Point2_Y
    }
    else {
      Min_Y = Rect_Point2_Y
      Max_Y = Rect_Point1_Y
    }

    // Determine if the point is contained inside of the rectangle (Boundary inclusive)
    if (Point_X >= Min_X && Point_X <= Max_X && Point_Y >= Min_Y && Point_Y <= Max_Y)
      return true
    else
      return false
  }

  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {

    // Extract the coordinates of pointString1
    var Point1 = pointString1.split(",")
    val Point1_X = Point1(0).toDouble
    val Point1_Y = Point1(1).toDouble

    // Extract the coordinates of pointString2
    var Point2 = pointString2.split(",")
    var Point2_X = Point2(0).toDouble
    var Point2_Y = Point2(1).toDouble

    // Compute the Euclidean Distance between Point1 and Point2
    val PointDistance = math.sqrt(math.pow((Point2_X - Point1_X), 2) + math.pow((Point2_Y - Point1_Y), 2))

    // Determine if the two points are within the given distance (Boundary inclusive)
    if (PointDistance <= distance)
      return true
    else
      return false
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
