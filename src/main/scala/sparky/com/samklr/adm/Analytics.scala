package sparky.com.samklr.adm

import com.spatial4j.core.context.SpatialContext
import com.spatial4j.core.shape.SpatialRelation
import com.spatial4j.core.shape.impl.PointImpl
import com.spatial4j.core.distance.DistanceUtils.KM_TO_DEG
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{udf, lit}
import org.apache.spark.sql.functions._
import sparky.MyKryoRegistrator
import org.apache.log4j.Logger
import org.apache.log4j.Level
/**
 * Created by samklr on 20/08/15.
 */
object Analytics extends App{

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
//  Logger.getLogger("parquet.hadoop").setLevel(Level.WARN)


  val conf = new SparkConf()
    .setAppName("Spark Template")
    .setMaster("spark://spark:7077")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
    .set("spark.executor.memory","16G")


  val sparkContext = new SparkContext(conf)

  val sqx = new org.apache.spark.sql.SQLContext(sparkContext)
  sqx.setConf("spark.sql.shuffle.partitions", "100");
  sqx.setConf("spark.sql.inMemoryColumnarStorage.batchSize","100000");
  sqx.setConf("spark.sql.parquet.compression.codec","snappy")
  sqx.setConf("spark.sql.parquet.filterPushdown","true")

  val parquetData_1M= "/media/samklr/windows/code/latticeiq/log_adm.1M.snappy.parquet/*"
  val parquetData_2M="/media/samklr/windows/code/latticeiq/log_adm_2M.snappy.parquet/*"
  val parquetData_1D="/media/samklr/windows/code/latticeiq/log_adm.full.log_20150720.snappy.parquet/*"
  val parquetData_4D="/home/samklr/data.adm.parquet/*"

  val logsDF = sqx.read.parquet(parquetData_4D).cache

  val cnt = logsDF.count

  println("Logs size : " + cnt)

  logsDF.registerTempTable("log_days")

  logsDF.printSchema()

  logsDF.show(50)

  //println("Size of parquet DF " + logsDF.count())
  //println("Executing Query 1")
  //val t1_start = System.currentTimeMillis()
  //println(q1.collectAsList())
  //val t1_end = System.currentTimeMillis()


  //Thread.sleep(8000)


  def within(latitude : Double, longitude : Double, centerLat : Double, centerLong : Double, radius : Double) : Boolean = {
    val ctx = SpatialContext.GEO
    val point = new PointImpl(latitude, longitude, ctx)
    val center = new PointImpl(centerLat, centerLong, ctx)

    SpatialRelation.WITHIN == point.relate(ctx.makeCircle(center, radius * KM_TO_DEG))
  }


  val withinUDF = udf(within _)

 sqx.udf.register("withinUDF", within _)

 val dataF = logsDF.filter(withinUDF(logsDF.col("lat"), logsDF.col("lon"),  lit(48.8436), lit(2.3238), lit(50.0)))
                  .select("log_day")
                  .groupBy("log_day")
                  .count

  val dataF2 = logsDF.select("log_day").groupBy("log_day").count


  val dataF3 = logsDF.select("log_day","pubuid").groupBy("log_day","pubuid")
                     .count

  val t1_start = System.currentTimeMillis()
  val dataF4 = logsDF.filter(withinUDF(logsDF.col("lat"), logsDF.col("lon"),  lit(48.8436), lit(2.3238), lit(10.0)))
                     .select("log_day", "userid", "impflag").groupBy("log_day")
                     .agg(countDistinct("userid") ,count("log_day"), sum("impflag") )

  println("dataF4 : " + dataF4.collectAsList)
  val t1_stop = System.currentTimeMillis();

  val t2_start = System.currentTimeMillis()
  val q1 =  sqx.sql("SELECT log_day, count(distinct userid) users, COUNT(*) impressions, sum(impflag) impflags FROM log_days WHERE withinUDF(lat, lon,  48.8436, 2.3238, 10.0) GROUP BY log_day")
  println("q2 sql : " + q1.collectAsList())
  val t2_stop = System.currentTimeMillis()


  dataF.show
  dataF2.show
  dataF3.show
  dataF4.show
  q1.show()


  println("dataF4 in : " + (t1_stop-t1_start)/1000.0 +" secs")
  println("sql q1 done in : " + (t2_stop-t2_start)/1000.0 +" secs")

  //"SELECT log_day, count(distinct userid) users, COUNT(*) , sum(impflags) GROUPBY log_day

  //val query2 = logsDF.


  //println("Filtered size " +dataF.count)
 // val t2_end = System.currentTimeMillis()

 // q1.show()

   // println("Query 1 done in : " + (t1_end-t1_start)/1000.0 +" secs")



  //  println("Query 2 done in : " + (t2_end-t2_start)/1000.0 +" secs")

   //                                                                                                                       dataF.foreach(println)


  /**

  def within(latitude : Double, longitude : Double, centerLat : Double, centerLong : Double, radius : Double) : Boolean = {
    val ctx = SpatialContext.GEO
    val point = new PointImpl(latitude, longitude, ctx)
    val center = new PointImpl(centerLat, centerLong, ctx)

    SpatialRelation.WITHIN == point.relate(ctx.makeCircle(center, radius * KM_TO_DEG))
  }


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
