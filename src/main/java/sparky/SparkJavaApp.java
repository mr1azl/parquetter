package sparky;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by samklr on 20/08/15.
 */
public class SparkJavaApp {


    public void sparkJob(){
        SparkConf sparkConf = new SparkConf().setAppName("Java spark, beurk !!!")
                                             .setMaster("local[*]")   ;

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        String file = "/etc/passwd";

        JavaRDD<String> data = jsc.textFile(file);

        long numAs = data.filter(line -> line.contains("a")).count();
        long numBs = data.filter(line -> line.contains("b")).count();


        System.out.println("Lines with a: "+numAs+", Lines with b: "+numBs);

        //WordCount
        JavaPairRDD<String, Integer> pairs = data.flatMap(line -> Arrays.asList(line.split(" ")))
                      .mapToPair(w -> {
                          return new Tuple2<String, Integer>(w, 1);
                      })
                      .reduceByKey((x, y) -> x + y);

        long counts = pairs.count();


        System.out.println("Passwd wc : " + counts);



    }


    public static void main(String[] args ) {
        new SparkJavaApp().sparkJob();

    }
}
