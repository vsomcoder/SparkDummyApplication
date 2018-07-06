/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparkwordcount;

import java.io.IOException;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author kamal
 */
public class WordCountTest {
    
    private JavaSparkContext sc;

    
    public WordCountTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void init() throws IllegalArgumentException, IOException {
        //ctxtBuilder = new ContextBuilder(tempFolder);
        
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("junit");
        sc = new JavaSparkContext(conf); 
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class WordCount.
     */
    @Test
    public void test() {
        WordCount  wc = new WordCount();
        JavaPairRDD<String, Integer> rdd;
        rdd = wc.getCounts(sc.parallelize(Arrays.asList("a","a"))); 
        long a = rdd.collect().size();
        assertEquals(1, a);

    }
    
}
