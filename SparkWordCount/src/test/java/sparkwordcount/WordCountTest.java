/*
 * This just test getCounts function. Filename  "*Test" are run by maven at test
 * stage and these are unit test i.e run before packaging is done 
 * or before jar is bundeled.
 * test 
 */
package sparkwordcount;

import java.io.IOException;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
    
    private static WordCount wc;

    
    public WordCountTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("junit");
        wc = new WordCount(conf); 
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void init() throws IllegalArgumentException, IOException {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of main method, of class WordCount.
     */
    @Test
    public void test() {
        JavaPairRDD<String, Integer> rdd;
        rdd = wc.getCounts(wc.getContext().parallelize(Arrays.asList("a","a"))); 
        long a = rdd.collect().size();
        assertEquals(1, a);

    }
    
}
