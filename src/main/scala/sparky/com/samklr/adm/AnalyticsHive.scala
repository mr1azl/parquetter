package sparky.com.samklr.adm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import sparky.MyKryoRegistrator

/**
 * Created by samklr on 22/08/15.
 */
object AnalyticsHive extends App{


  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf()
    .setAppName("Spark Template")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
    .set("spark.executor.memory","2G")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.speculation","true")
    .set("spark.broadcast.blockSize","8m")
    .set("spark.default.parallelism","48")


  val sparkContext = new SparkContext(conf)

  val hiveContext = new HiveContext(sparkContext)

  hiveContext.setConf("spark.sql.shuffle.partitions", "4");
  hiveContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize","40000")
  hiveContext.setConf("spark.sql.parquet.compression.codec","snappy")
  hiveContext.setConf("spark.sql.parquet.filterPushdown","true")
  hiveContext.setConf("spark.sql.codegen","true")

  hiveContext.sql("add jar lib/esri-geometry-api-1.2.1.jar")
  hiveContext.sql("add jar lib/spatial-sdk-json-1.1.1.jar")
  hiveContext.sql("add jar lib/spatial-sdk-hive-1.1.1.jar")

  hiveContext.sql("create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point' ")
  hiveContext.sql("create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains'")
  hiveContext.sql("create temporary function ST_Buffer as 'com.esri.hadoop.hive.ST_Buffer'")
  hiveContext.sql("create temporary function ST_LineString as 'com.esri.hadoop.hive.ST_LineString'")
  hiveContext.sql("create temporary function ST_Length as 'com.esri.hadoop.hive.ST_Length'")
  hiveContext.sql("create temporary function ST_GeodesicLengthWGS84 as 'com.esri.hadoop.hive.ST_GeodesicLengthWGS84'")
  hiveContext.sql("create temporary function ST_SetSRID as 'com.esri.hadoop.hive.ST_SetSRID'")
  hiveContext.sql("create temporary function ST_Polygon as 'com.esri.hadoop.hive.ST_Polygon' ")
  hiveContext.sql("create temporary function ST_Intersects as 'com.esri.hadoop.hive.ST_Intersects'")

  val parquetData_1M= "/media/samklr/windows/code/latticeiq/log_adm.1M.snappy.parquet/*"
  val parquetData_2M="/media/samklr/windows/code/latticeiq/log_adm_2M.snappy.parquet/*"
  val parquetData_1D="/media/samklr/windows/code/latticeiq/log_adm.full.log_20150720.snappy.parquet/*"
  val parquetData_4D="/home/samklr/data.adm.parquet/*"

  val logsDF = hiveContext.read.parquet(parquetData_1M)
  logsDF.registerTempTable("log_days")
  hiveContext.cacheTable("log_days")


  val query1 = "SELECT log_day, count(distinct userid) users, COUNT(*) impressions, sum(impflag) FROM log_days  WHERE ST_Contains ((ST_BUFFER(ST_Point(4.835658,45.764043), 20)), ST_Point(lon, lat)) GROUP BY log_day "
  val query2 = "SELECT log_day, COUNT(*) impressions FROM log_days  WHERE ST_Contains ((ST_BUFFER(ST_Point(4.835658,45.764043), 20)), ST_Point(lon, lat)) GROUP BY log_day"


  val t1_start = System.nanoTime
  hiveContext.sql(query1).show
  val t1_end = System.nanoTime
  println ("Query 1 took : "+ (t1_end - t1_start)/1000000000.0)


  val t2_start = System.nanoTime
  hiveContext.sql(query2).show
  val t2_end = System.nanoTime
  println ("Query 2 took : "+ (t2_end - t2_start)/1000000000.0)

}
