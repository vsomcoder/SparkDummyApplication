/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparkwordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.SparkHadoopUtil;
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

    private static JavaSparkContext sc;

    public WordCountIT() {
    }

    @BeforeClass
    public static void setUpClass() {
        String hadoop_master = System.getProperty("hadoop_master");
        String env_type = System.getProperty("env_type");
        
        SparkConf conf = new SparkConf();
        Configuration hadoop_conf = new Configuration();
        hadoop_conf.set("spark.hadoop.fs.defaultFS", "hdfs://" + hadoop_master + ":9000");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        
        conf.setMaster("yarn");
        conf.set("spark.yarn.access.namenodes","hdfs://" + hadoop_master + ":9000");
        conf.set("spark.hadoop.fs.defaultFS", "hdfs://" + hadoop_master + ":9000");
        conf.set("spark.hadoop.yarn.resourcemanager.hostname", hadoop_master);
        conf.set("spark.hadoop.hadoop.security.authentication", "kerberos");
        conf.set("spark.hadoop.hadoop.security.authorization", "true");
        conf.setSparkHome("/usr/local/spark");
        conf.setAppName("junit");
        String[] jars = {"target/" + System.getProperty("finalName") + ".jar"};
        conf.setJars(jars);
        if ("aws".equals(env_type)) {
            try {
                UserGroupInformation.setConfiguration(hadoop_conf); 
                UserGroupInformation.loginUserFromKeytab("kamal@SKAMALJ.AWS", "/home/ubuntu/kamal.keytab");
            } catch (IOException ex) {
                Logger.getLogger(WordCountIT.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        sc = new JavaSparkContext(conf);
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
        WordCount wc = new WordCount();
        JavaPairRDD<String, Integer> rdd, result_rdd;
        List<Tuple2<String, Integer>> result_list;
        result_list = new ArrayList();
        result_list.add(new Tuple2<>("a", 3));
        result_list.add(new Tuple2<>("b", 6));
        result_list.add(new Tuple2<>("c", 1));
        result_list.add(new Tuple2<>("d", 2));
        result_list.add(new Tuple2<>("f", 1));
        result_list.add(new Tuple2<>("g", 1));
        rdd = wc.getCounts(sc.textFile("testspark"));
        result_rdd = JavaPairRDD.fromJavaRDD(sc.parallelize(result_list));
        long a = rdd.subtract(result_rdd).collect().size();
        assertEquals(0, a);

    }

    @Test
    public void test_two() {
        WordCount wc = new WordCount();
        JavaPairRDD<String, Integer> rdd, result_rdd;
        List<Tuple2<String, Integer>> result_list;
        result_list = new ArrayList();
        result_list.add(new Tuple2<>("a", 3));
        result_list.add(new Tuple2<>("b", 6));
        result_list.add(new Tuple2<>("c", 1));
        result_list.add(new Tuple2<>("d", 2));
        result_list.add(new Tuple2<>("f", 1));
        result_list.add(new Tuple2<>("g", 2));
        rdd = wc.getCounts(sc.textFile("testspark"));
        result_rdd = JavaPairRDD.fromJavaRDD(sc.parallelize(result_list));
        long a = rdd.subtract(result_rdd).collect().size();
        assertEquals(1, a);

    }

    @Test
    public void test_three() {
        WordCount wc = new WordCount();
        JavaPairRDD<String, Integer> rdd, result_rdd;
        List<Tuple2<String, Integer>> result_list;
        result_list = new ArrayList();
        result_list.add(new Tuple2<>("a", 3));
        result_list.add(new Tuple2<>("b", 6));
        result_list.add(new Tuple2<>("c", 1));
        result_list.add(new Tuple2<>("d", 2));
        result_list.add(new Tuple2<>("f", 1));
        result_list.add(new Tuple2<>("g", 1));
        rdd = wc.getCounts(sc.textFile("testspark"));
        result_rdd = JavaPairRDD.fromJavaRDD(sc.parallelize(result_list));
        long a = rdd.union(result_rdd).collect().size();
        assertEquals(12, a);

    }

}
