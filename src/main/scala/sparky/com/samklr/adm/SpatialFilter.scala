package spark.com.samklr.adm


import com.spatial4j.core.context.SpatialContext
import com.spatial4j.core.shape.SpatialRelation
import com.spatial4j.core.shape.impl.PointImpl
import com.spatial4j.core.distance.DistanceUtils.KM_TO_DEG

object SpatialFilter extends App {

    def within(latitude : Double, longitude : Double, centerLat : Double, centerLong : Double, radius : Double) : Boolean = {
      val ctx = SpatialContext.GEO
      val point = new PointImpl(latitude, longitude, ctx)
      val center = new PointImpl(centerLat, centerLong, ctx)

      SpatialRelation.WITHIN == point.relate(ctx.makeCircle(center, radius * KM_TO_DEG))
    }

    //using wkt
    def inCenterSphere (point : String, origin : String, radius : Double) = {
      // val sphere

    }


  /**
  val ctx = SpatialContext.GEO

  val pt = ctx.readShapeFromWkt("Point(48.8539 2.3344")

  val saint_germain = new PointImpl(48.8539, 2.3344, ctx);
  val montparnasse = new PointImpl(48.8436, 2.3238, ctx)
  **/




}
