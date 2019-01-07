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
  pickupInfo.show()

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
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
  
  pickupInfo.createOrReplaceTempView("pickupInfo")  
  
  val reqPoints = spark.sql("select x,y,z,count(*) as countVal from pickupInfo where x>=" + minX + " and x<=" + maxX + " and y>="+minY +" and y<="+maxY+" and z>="+minZ+" and z<=" +maxZ +" group by x,y,z").persist()
  reqPoints.createOrReplaceTempView("reqPoints")    
    
  val p = spark.sql("select sum(countVal) as sumVal, sum(countVal*countVal) as sumSqr from reqPoints").persist()
  val sumVal = p.first().getLong(0).toDouble
  val sumSqr = p.first().getLong(1).toDouble  
  
  val mean = (sumVal/numCells)
  val stnd_deviation = Math.sqrt((sumSqr/numCells) - (mean*mean))   
  
  val ifNeighbor = spark.sql("select gp1.x as x , gp1.y as y, gp1.z as z, count(*) as numOfNb, sum(gp2.countVal) as sigma from reqPoints as gp1 inner join reqPoints as gp2 on ((abs(gp1.x-gp2.x) <= 1 and  abs(gp1.y-gp2.y) <= 1 and abs(gp1.z-gp2.z) <= 1)) group by gp1.x, gp1.y, gp1.z").persist()
  ifNeighbor.createOrReplaceTempView("ifNeighbor")
  
  spark.udf.register("CalculateZScore",(mean: Double, stddev:Double, numOfNb: Int, sigma: Int, numCells:Int)=>((
    HotcellUtils.CalculateZScore(mean, stddev, numOfNb, sigma, numCells)
    )))  
  
  val withZscore =  spark.sql("select x,y,z,CalculateZScore("+ mean + ","+ stnd_deviation +",numOfNb,sigma," + numCells+") as zscore from ifNeighbor")
  withZscore.createOrReplaceTempView("withZscore")
  
  val retVal = spark.sql("select x,y,z from withZscore order by zscore desc")
  return retVal
}

}
