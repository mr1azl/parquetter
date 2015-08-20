package sparky.com.samklr.adm

import com.google.gson.Gson

/**
 * Created by samklr on 20/08/15.
 */
object Logs {

  case class RawLog ( coordinates : Array[Double],
                      impflag : Long,
                      log_date : String,
                      log_day: String,
                      log_hour : Long,
                      log_tkn : String,
                      pubuid : String,
                      userid : String )

  case class FlatLog (lat : Double,
                      lon : Double,
                      impflag : Long,
                      log_date : String,
                      log_day: String,
                      log_hour : Long,
                      log_tkn : String,
                      pubuid : String,
                      userid : String )


  def parseFromJson(lines:Iterator[String])={
    val gson = new Gson
    lines.map( line =>  gson.fromJson(line, classOf[RawLog]))
  }

}
