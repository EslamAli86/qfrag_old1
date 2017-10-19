import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by ehussein on 10/19/17.
 */
public class WordCount {
    SparkConf conf = null;
    JavaSparkContext sc = null;

    public WordCount() {
        conf = new SparkConf().setAppName("WordCountApp").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
    }

    public void compute() {
        JavaRDD<String> lines = sc.textFile("hdfs:///input/citeseer.graph");

        JavaRDD<String> words = lines.flatMap(
                s -> Arrays.asList(s.split(" ")).iterator()
        );

        JavaPairRDD<String, Integer> initCounts = words.mapToPair(s -> new Tuple2(s,1));
        JavaPairRDD<String, Integer> wordCounts = initCounts.reduceByKey((v1, v2) -> ((Integer)v1)+((Integer)v2));

        JavaRDD wordCountsSorted = wordCounts.map(x -> x).sortBy(v -> v._2, false, 1);
        wordCountsSorted.foreach(tuple -> {
            Tuple2<String, Integer> pair = (Tuple2<String, Integer>) tuple;
            System.out.println("Count(" + pair._1 + ") =  " + pair._2);
        });
    }

    public static void main(String[] args) {
        WordCount wc = new WordCount();
        wc.compute();
    }
}
