/*
 * This is a simple wordcount program written in Spark to demonstrate JUNIT use
 * to execute unit and integration tests. Program is split in three functions 
 * readfile, getCounts and saveOutput. 
 * getCount function  is tested in UT whereas IT tests readFile and getCounts.
 * Context setter and getter is written to enable testing from different environemnts.
 */
package sparkwordcount;

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
import scala.Tuple2;

/**
 *
 * @author kamal
 */
public class WordCount {

    JavaSparkContext sc;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().
                setAppName("wordcount").
                setMaster("yarn");        
        WordCount wc = new WordCount(conf);
        JavaRDD<String> line = wc.readFile(args[0]);
        wc.saveOutput(wc.getCounts(line), args[1]);
    }

    WordCount(SparkConf conf) {
        sc = new JavaSparkContext(conf);
    }
    
    JavaSparkContext getContext() {
        return sc;
    }
    
    JavaRDD<String> readFile(String file) {
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
