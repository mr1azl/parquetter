package sparky.com.samklr.adm

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions.{udf, lit}
import org.apache.spark.sql._

import sparky.MyKryoRegistrator


object Preprocess extends App {

     val data = "/home/samklr/code/latticeiq/adm-spark-server/data/log_adm.json"
     val parquet_dir = "/home/samklr/code/latticeiq/parquet.parts.200K"

     val conf = new SparkConf()
       .setAppName("Parquetter")
       .setMaster("local[*]")
       .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       .set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
       .set("spark.executor.memory","1G")
       .set("spark.scheduler.mode", "FAIR")
       .set("spark.speculation","true")
       .set("spark.broadcast.blockSize","8m")
       .set("spark.default.parallelism","16")

     val sparkContext = new SparkContext(conf)
     val sqlContext = new SQLContext(sparkContext)

     sqlContext.setConf("spark.sql.shuffle.partitions", "4");
     sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize","100000");
     sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
     sqlContext.setConf("spark.sql.parquet.filterPushdown","true")

     println(sparkContext.getConf.toDebugString)


    def mkPoint = udf( (a : Seq[Double]) => s"Point(${a(1)} ${a(0)})")

    def mkString = udf((a: Seq[Double]) => a.mkString(", "))

    def mk2Columns =  udf((a: Seq[Double], i: Int) => a(i))

    def saveParquet(df : DataFrame) = df.write.format("parquet")
                                              .partitionBy("log_day","log_hour","pubuid")
                                              .save(parquet_dir)


    import sqlContext.implicits._

    val raw_data = sqlContext.read.format("json").load(data).na.drop

    val logsWithStringCoordinates = raw_data.withColumn("coord_string", mkString($"coordinates"))

    val logsWith2CoordColumns = raw_data.select($"*", mk2Columns($"coordinates", lit(1)).alias("lat"),
                                                        mk2Columns($"coordinates", lit(0)).alias("lon")
                                                )

    val logsWithWkt  = raw_data.withColumn("coords", mkPoint($"coordinates"))

    logsWith2CoordColumns.printSchema
    logsWithStringCoordinates.printSchema
    logsWithWkt.printSchema

    logsWith2CoordColumns.show(5)
    logsWithStringCoordinates.show(5)
    logsWithWkt.show(5)

    println(logsWith2CoordColumns.count)
    println(logsWithStringCoordinates.count)
    println(logsWithWkt.count)

}
