package sparky

/**
 * Created by samklr on 20/08/15.
 */


import com.esotericsoftware.kryo.Kryo
import org.apache.spark.SparkConf

class MyKryoRegistrator extends KryoSerializer {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Models.SomeData])
    //Add your classes that you want to kryolize below

  }
}

object MyKryoRegistrator {
  def register(conf: SparkConf) {
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[MyKryoRegistrator].getName)
  }
}
