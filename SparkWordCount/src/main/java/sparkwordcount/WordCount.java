/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparkwordcount;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.Utils;
import scala.Tuple2;

/**
 *
 * @author kamal
 */
public class WordCount {

    static JavaSparkContext sc;

    public static void main(String[] args) throws Exception {

        WordCount wc = new WordCount();

        SparkConf conf = new SparkConf().setAppName("wordcount");
        conf.setMaster("yarn");

        JavaRDD<String> line = wc.readFile(args[0], conf);

        wc.saveOutput(wc.getCounts(line), args[1]);

    }

    JavaRDD<String> readFile(String file, SparkConf conf) {
        sc = new JavaSparkContext(conf);
        return sc.textFile(file);
    }

    JavaPairRDD<String, Integer> getCounts(JavaRDD<String> line) {
        JavaPairRDD<String, Integer> counts = line
                .flatMap(l -> Arrays.asList(l.split(":")).iterator())
                .mapToPair(w -> new Tuple2(w, 1))
                .reduceByKey((x, y) -> (Integer) x + (Integer) y);
        return counts;
    }

    void saveOutput(JavaPairRDD<String, Integer> counts, String outfile) {
        try {
            FileSystem hdfs = FileSystem.get(sc.hadoopConfiguration());
            hdfs.delete(new Path(outfile), true);
            counts.saveAsTextFile(outfile);
        } catch (IOException ex) {
            Logger.getLogger(WordCount.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
