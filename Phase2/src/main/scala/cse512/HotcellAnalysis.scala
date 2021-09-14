package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  //pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  //pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo = pickupInfo.select("x", "y", "z").where("x >= " + minX + " AND y >= " + minY + " AND z >= " + minZ + " AND x <= " + maxX + " AND y <= " + maxY + " AND z <= " + maxZ)
  pickupInfo = pickupInfo.groupBy("z", "y", "x").count()

  val mean = (pickupInfo.agg(sum("count")).first().getLong(0).toDouble) / numCells
  val sd = scala.math.sqrt((pickupInfo.withColumn("square", pow(col("count"), 2)).agg(sum("square")).first().getDouble(0) / numCells) - scala.math.pow(mean, 2))

  pickupInfo.createOrReplaceTempView("temp")
  val temp_df = spark.sql("SELECT first.x AS x, first.y AS y, first.z AS z, sum(second.count) AS sum_of_cells, count(second.count) as count_of_cells "
        + "FROM temp AS first, temp AS second "
        + "WHERE (second.x = first.x+1 OR second.x = first.x OR second.x = first.x-1) AND (second.y = first.y+1 OR second.y = first.y OR second.y = first.y-1) AND (second.z = first.z+1 OR second.z = first.z OR second.z = first.z-1)"
        + "GROUP BY first.z, first.y, first.x")
  
  var calculate_g_score = udf((mean: Double, sd: Double, numCells: Double, sum_of_cells: Long, count_of_cells: Long) => HotcellUtils.g_score(mean, sd, numCells, sum_of_cells, count_of_cells))
  val result = temp_df.withColumn("G_score", calculate_g_score(lit(mean), lit(sd), lit(numCells), col("sum_of_cells"), col("count_of_cells")))
                .orderBy(desc("G_score")).limit(50)

  result.show()
  val output = result.select("x", "y", "z")
  return output // YOU NEED TO CHANGE THIS PART
  }
}
