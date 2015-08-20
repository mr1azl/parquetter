package sparky.com.samklr.adm

import org.apache.spark.{SparkConf, SparkContext}
import sparky.MyKryoRegistrator

/**
 * Created by samklr on 20/08/15.
 */
object Analytics extends App{

  val conf = new SparkConf()
    .setAppName("Spark Template")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.executor.memory","3G")



  val sparkContext = new SparkContext(conf)

  val sqx = new org.apache.spark.sql.SQLContext(sparkContext)
  sqx.setConf("spark.sql.shuffle.partitions", "100");
  sqx.setConf("spark.sql.inMemoryColumnarStorage.batchSize","50000");
  sqx.setConf("spark.sql.parquet.compression.codec","snappy")


  val parquetData_1D ="/media/samklr/windows/code/latticeiq/log_20150720.parquet/*"
  val parquetData_1M= "/media/samklr/windows/code/latticeiq/log_adm_1M.parquet/*"

  val logsDF = sqx.read.parquet(parquetData_1M).cache

  logsDF.registerTempTable("log_days")

  //logsDF.printSchema()

  //logsDF.show(20)

  //println("Size of parquet DF " + logsDF.count())
  println("Executing Query 1")
  val t1_start = System.currentTimeMillis()
  val q1 =  sqx.sql("SELECT log_day, count(userid) users, COUNT(*) impressions,sum(impflag) impflags FROM log_days GROUP BY log_day")

  q1.show()
  val t1_end = System.currentTimeMillis()

  println("Query 1 done in : " + (t1_end-t1_start)/1000.0 +" secs")



/**

  def within(latitude : Double, longitude : Double, centerLat : Double, centerLong : Double, radius : Double) : Boolean = {
    val ctx = SpatialContext.GEO
    val point = new PointImpl(latitude, longitude, ctx)
    val center = new PointImpl(centerLat, centerLong, ctx)

    SpatialRelation.WITHIN == point.relate(ctx.makeCircle(center, radius * KM_TO_DEG))
  }


  //udf

  val withinUDF = udf(within _)


  //df.where(withinUDF($"lat", $"long", lit(RADIUS)))
  //def within(radius: Double) = udf((lat: Double, long: Double) => ???)
  //df.where(within(RADIOUS)($"lat", $"long"))



  val geoDF = flatDF.where(withinUDF($"lat", $"lon",  lit(48.8436), lit(2.3238), lit(10.0)))

  geoDF.registerTempTable("log_days")

  geoDF.count

  val quer1 = sqlContext.sql("SELECT log_day, COUNT(*) impressions FROM log_days GROUP BY log_day")

  val query2 = sqlContext.sql("SELECT log_day, count(distinct userid) users, COUNT(*) impressions,sum(impflag) impflags FROM log_days GROUP BY log_day")
  **/



}
