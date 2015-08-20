package sparky

/**
 * Created by samklr on 20/08/15.
 */


import _root_.com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkConf
import sparky.com.samklr.adm.Logs.{FlatLog, RawLog}

class MyKryoRegistrator extends KryoSerializer {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[RawLog])
    kryo.register(classOf[FlatLog])

  }
}

object MyKryoRegistrator {
  def register(conf: SparkConf) {
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
  }
}
