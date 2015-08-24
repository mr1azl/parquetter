package sparky.com.samklr.adm

import java.util.concurrent.TimeUnit

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
    //.setMaster("spark://spark:7077")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
    .set("spark.executor.memory","2G")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.speculation","true")
    .set("spark.broadcast.blockSize","8m")
    .set("spark.default.parallelism","16")
    .set("spark.broadcast.compress", "false") //only in local mode
    .set("spark.shuffle.compress", "false") //only in local mode
    .set("spark.shuffle.spill.compress", "false") //only in local mode



  val sparkContext = new SparkContext(conf)

  val sqx = new org.apache.spark.sql.SQLContext(sparkContext)
  sqx.setConf("spark.sql.shuffle.partitions", "2"); //change when running in cluster
  sqx.setConf("spark.sql.inMemoryColumnarStorage.batchSize","10000");
  sqx.setConf("spark.sql.parquet.compression.codec","snappy")
  sqx.setConf("spark.sql.parquet.filterPushdown","true")
  sqx.setConf("spark.sql.codegen","true")

  val parquetData_1M= "/media/samklr/windows/code/latticeiq/log_adm.1M.parts.parquet/"
  val parquetData_2M="/media/samklr/windows/code/latticeiq/log_adm_2M.snappy.parquet/*"
  val parquetData_1D="/media/samklr/windows/code/latticeiq/log_adm.full.log_20150720.snappy.parquet/*"
  val parquetData_4D="/home/samklr/data.adm.parquet/*"

  val logsDF = sqx.read
                  .option("mergeSchema", "false")
                  .parquet(parquetData_1M)
                  .cache


  logsDF.registerTempTable("log_days")

  logsDF.printSchema()

  logsDF.show(50)


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

  println("dataF : " +dataF.collectAsList)

  val dataF2 = logsDF.select("log_day").groupBy("log_day").count
  println("dataF2 : " +dataF2.collectAsList)


  val dataF3 = logsDF.select("log_day","pubuid").groupBy("log_day","pubuid").count
  println("dataF3 : " +dataF3.collectAsList)


  val dataF5 = logsDF.select("log_day","pubuid","userid", "impflag").groupBy("log_day","pubuid","userid").count
  println("dataF5 : " +dataF5.collectAsList)

  val t1_start = System.nanoTime()
  val dataF4 = logsDF.filter(withinUDF(logsDF.col("lat"), logsDF.col("lon"),  lit(48.8436), lit(2.3238), lit(10.0)))
                     .select("log_day", "userid", "impflag").groupBy("log_day")
                     .agg(countDistinct("userid") ,count("log_day"), sum("impflag") )

  println("dataF4 : " + dataF4.collectAsList)

  val t1_stop = System.nanoTime()

  val t2_start = System.nanoTime()
  val q1 =  sqx.sql("SELECT log_day, count(distinct userid) users, COUNT(*) impressions, sum(impflag) impflags FROM log_days WHERE withinUDF(lat, lon,  48.8436, 2.3238, 10.0) GROUP BY log_day")
  println("q2 sql : " + q1.collectAsList())
  val t2_stop = System.nanoTime()


  dataF.show
  dataF2.show
  dataF3.show
  dataF4.show
  dataF5.show
  q1.show

  val elapsed1 = TimeUnit.NANOSECONDS.toSeconds(t1_stop  - t1_start)
  val elapsed2 = TimeUnit.NANOSECONDS.toSeconds(t2_stop  - t2_start)


  println("dataF4 in : " + elapsed1 +" secs")
  println("sql q1 done in : " + elapsed2 +" secs")

}
