package big_data_analytics_java.chp1;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PairedRDDExample {

	private static String appName = "PairedRddExample";
	private static String master = "local[*]";
	
	public static void main(String[] args) {
		LogManager.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rddX = sc.parallelize(
                Arrays.asList("videoName1,5","videoName2,6", "videoName3,2","videoName1,6"));

		System.out.println("--------- videoCountTupleRdd -----------");
		JavaRDD<Tuple2<String, Integer>> videoCountTupleRdd = rddX.map((String s) -> {
			String[] arr = s.split(",");
			return new Tuple2<String, Integer>(arr[0], Integer.parseInt(arr[1]));
		});
		videoCountTupleRdd.foreach(t -> System.out.println(t));

		System.out.println("--------- pairRdd -----------");
		JavaPairRDD<String, Integer> pairRdd = videoCountTupleRdd.mapToPair(t -> {
			return new Tuple2<String, Integer>(t._1, t._2);
		});
		pairRdd.foreach(t -> System.out.println(t._1));

		System.out.println("--------- videoCountPairRdd -----------");
		JavaPairRDD<String, Integer> videoCountPairRdd = rddX.mapToPair((String s) -> {
			String[] arr = s.split(",");
			return new Tuple2<String, Integer>(arr[0], Integer.parseInt(arr[1]));
		});
		videoCountPairRdd.foreach(t -> System.out.println(t._1));

		System.out.println("--------- sumPairRdd -----------");
		JavaPairRDD<String, Integer> sumPairRdd = videoCountPairRdd.reduceByKey((a,b) -> (a-1) * (b-2));
		sumPairRdd.foreach(tuple -> System.out.println("Title : " + tuple._1 + ", Hit Count : " + tuple._2));
/*		List<Tuple2<String,Integer>> testResults = sumPairRdd.collect();
		for (Tuple2<String, Integer> tuple2 : testResults) {
			System.out.println("Title : " + tuple2._1 + ", Hit Count : " + tuple2._2);
		}*/

		System.out.println("--------- grpPairRdd -----------");
		JavaPairRDD<String, Iterable<Integer>> grpPairRdd = videoCountPairRdd.groupByKey();
		
		List<Tuple2<String,Iterable<Integer>>> testResults1 = grpPairRdd.collect();
		for (Tuple2<String, Iterable<Integer>> tuple2 : testResults1) {
			
			System.out.println("Title : " + tuple2._1  );
			Iterator<Integer> it = tuple2._2.iterator();
			int i = 1;
			while(it.hasNext()) {
				System.out.println("value " + i + " : " + it.next());
				i++;
			}
		}

		//grpPairRdd.coalesce(1).saveAsTextFile("output2.txt");

/*		System.out.println("--------- grpRdd -----------");
		JavaRDD<Tuple2<String,Iterable<Integer>>> grpRdd = grpPairRdd.fold();*/
	}

}
