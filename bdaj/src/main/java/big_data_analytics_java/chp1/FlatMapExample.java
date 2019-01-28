package big_data_analytics_java.chp1;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class FlatMapExample {

	private static String appName = "LOAD_DATA_APPNAME";
	private static String master = "local";
	private static String FILE_NAME = "data/university-rankings/school_and_country_table.csv";
	
	public static void main(String[] args) {
		LogManager.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rddX = sc.parallelize(
                Arrays.asList("big data","analytics", "using java"));
		 JavaRDD<String[]> rddY = rddX.map(e -> e.split(" "));
         JavaRDD<String> rddY2 = rddX.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
	        List<String> listUsingFlatMap = rddY2.collect();
	        System.out.println(listUsingFlatMap);
		System.out.println("-----------------------------");
		rddY2 = rddY.flatMap(e -> Arrays.asList(e).iterator());
		System.out.println(rddY2.collect());
	}

}
