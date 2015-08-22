package sparky.com.samklr.adm

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import sparky.MyKryoRegistrator
import sparky.com.samklr.adm.Logs.FlatLog
import sparky.com.samklr.adm.Logs.RawLog
import _root_.com.esotericsoftware.kryo.Kryo

/**
 * Created by samklr on 20/08/15.
 */
object Transformer extends App{

  val conf = new SparkConf()
    .setAppName("Parquetter")
    .setMaster("spark://spark:7077")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
    .set("spark.executor.memory","10G")

  val sparkContext = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
  sqlContext.setConf("spark.sql.shuffle.partitions", "100");
  sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize","100000");
  sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
  sqlContext.setConf("spark.sql.parquet.filterPushdown","true")


  println(sparkContext.getConf.toDebugString)


 val flat_logs = sparkContext.textFile("/home/samklr/data/*",100)
                             .mapPartitions(line => Logs.parseFromJson(line))
                             .map(r => if ((r != null) && (r.coordinates != null))
                                          FlatLog( r.coordinates(1), r.coordinates(0), r.impflag, r.log_date,
                                                r.log_day, r.log_hour, r.log_tkn, r.pubuid, r.userid)
                                          else FlatLog (0.0, 0.0, 0, "NA", "NA", 0, "NA","NA", "NA"))
                             .filter (log => (log.userid != "NA") || (log.lat != 0.0) || (log.lon != 0) || (log.pubuid != "NA"))
                             .persist(StorageLevel.MEMORY_AND_DISK_SER)

  val flatDF=sqlContext.createDataFrame[FlatLog](flat_logs).na.drop

 // flatDF.printSchema()

  //flatDF.show(10)

  val sz= flatDF.count

  println("DataFrame size " + sz)

  flatDF.write
        .format("parquet")
        .partitionBy("log_day")
        .save("/home/samklr/data.adm.parquet")

  //flatDF.write.parquet("/home/samklr/data.parq.lz")

}
