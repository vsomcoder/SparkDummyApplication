/*
 * This function tests all 3 functions of wordcount program. Parameter hadoop_master
 * and jar name to the program is injected by maven (POM file)
 * "*IT" tests are run by maven during verify stage which comes after the packaging
 * is completed and jar is available.
 */
package sparkwordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import scala.Tuple2;

/**
 *
 * @author kamal
 */
public class WordCountIT {

    private static WordCount wc;

    public WordCountIT() {
    }

    @BeforeClass
    public static void setUpClass() {
        String hadoop_master = System.getProperty("hadoop_master");
        String env_type = System.getProperty("env_type");

        
        SparkConf conf = new SparkConf();
        
        conf.set("spark.hadoop.fs.defaultFS", "hdfs://" + hadoop_master + ":9000");
        conf.set("spark.hadoop.yarn.resourcemanager.hostname", hadoop_master);
        conf.setSparkHome("/usr/local/spark");
        conf.setAppName("junit");
        String[] jars = {"target/" + System.getProperty("finalName") + ".jar"};
        conf.setJars(jars);
        conf.setMaster("yarn");
        wc = new WordCount(conf);
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void init() throws IllegalArgumentException, IOException {
        //ctxtBuilder = new ContextBuilder(tempFolder);
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class WordCount.
     */
    @Test
    public void test_one() {
        JavaPairRDD<String, Integer> rdd, result_rdd;
        List<Tuple2<String, Integer>> result_list;
        result_list = new ArrayList();
        result_list.add(new Tuple2<>("a", 3));
        result_list.add(new Tuple2<>("b", 6));
        result_list.add(new Tuple2<>("c", 1));
        result_list.add(new Tuple2<>("d", 2));
        result_list.add(new Tuple2<>("f", 1));
        result_list.add(new Tuple2<>("g", 1));
        rdd = wc.getCounts(wc.readFile("testspark"));
        result_rdd = JavaPairRDD.fromJavaRDD(wc.getContext().parallelize(result_list));
        long a = rdd.subtract(result_rdd).collect().size();
        assertEquals(0, a);

    }

    @Test
    public void test_two() {
        JavaPairRDD<String, Integer> rdd, result_rdd;
        List<Tuple2<String, Integer>> result_list;
        result_list = new ArrayList();
        result_list.add(new Tuple2<>("a", 3));
        result_list.add(new Tuple2<>("b", 6));
        result_list.add(new Tuple2<>("c", 1));
        result_list.add(new Tuple2<>("d", 2));
        result_list.add(new Tuple2<>("f", 1));
        result_list.add(new Tuple2<>("g", 2));
        rdd = wc.getCounts(wc.readFile("testspark"));
        result_rdd = JavaPairRDD.fromJavaRDD(wc.getContext().parallelize(result_list));
        long a = rdd.subtract(result_rdd).collect().size();
        assertEquals(1, a);

    }

    @Test
    public void test_three() {
        JavaPairRDD<String, Integer> rdd, result_rdd;
        List<Tuple2<String, Integer>> result_list;
        result_list = new ArrayList();
        result_list.add(new Tuple2<>("a", 3));
        result_list.add(new Tuple2<>("b", 6));
        result_list.add(new Tuple2<>("c", 1));
        result_list.add(new Tuple2<>("d", 2));
        result_list.add(new Tuple2<>("f", 1));
        result_list.add(new Tuple2<>("g", 1));
        rdd = wc.getCounts(wc.readFile("testspark"));
        result_rdd = JavaPairRDD.fromJavaRDD(wc.getContext().parallelize(result_list));
        long a = rdd.union(result_rdd).collect().size();
        assertEquals(12, a);

    }
    
    @Test
    public void test_four() {
        JavaPairRDD<String, Integer>  result_rdd;
        List<Tuple2<String, Integer>> result_list;
        result_list = new ArrayList();
        result_list.add(new Tuple2<>("a", 3));
        result_list.add(new Tuple2<>("b", 6));
        result_list.add(new Tuple2<>("c", 1));
        result_list.add(new Tuple2<>("d", 2));
        result_list.add(new Tuple2<>("f", 1));
        result_list.add(new Tuple2<>("g", 1));        
        result_rdd = JavaPairRDD.fromJavaRDD(wc.getContext().parallelize(result_list));
        wc.saveOutput(result_rdd, "testsavewcfunction");
        long a = wc.getContext().textFile("testsavewcfunction").collect().size();
        assertEquals(6, a);

    }

}
