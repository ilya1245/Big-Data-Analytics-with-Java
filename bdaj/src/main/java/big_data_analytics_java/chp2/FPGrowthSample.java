package big_data_analytics_java.chp2;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.api.java.function.Function;

public class FPGrowthSample {
    private static String appName = "FpGrowth_Example";
    private static String master = "local[*]";

    public static void main(String[] args) {
        LogManager.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile("resources/data/retail/retail_small_fpgrowth.txt");

        System.out.println("---------- transactions ----------");
        JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(" ")));
        transactions.foreach(f -> System.out.println(f));

        System.out.println("---------- fpGrowth model ----------");
        FPGrowth fpGrowth = new FPGrowth().setMinSupport(0.4).setNumPartitions(1);
        FPGrowthModel<String> model = fpGrowth.run(transactions);
        model.freqItemsets().toJavaRDD().foreach(itemset ->
                System.out.println("[" + itemset.javaItems() + "], " + itemset.freq()));

        System.out.println("---------- model rules ----------");
        double minConfidence = 0;
        model.generateAssociationRules(minConfidence).toJavaRDD().foreach(rule ->
                System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence()));
    }
}
