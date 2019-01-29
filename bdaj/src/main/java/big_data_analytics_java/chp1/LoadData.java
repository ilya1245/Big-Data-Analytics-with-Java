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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class LoadData {

    private static String appName = "LOAD_DATA_APPNAME";
    private static String master = "local";
    private static String FILE_NAME1 = "resources/data/university-rankings/school_and_country_table.csv";
    private static String FILE_NAME2 = "data/kc_house_data.csv";

    public static void main(String[] args) {
        LogManager.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> strRowRdd = sc.textFile(FILE_NAME1);
        JavaRDD<String> strRowRdd2 = sc.textFile(FILE_NAME1);
        //Dataset<Row> fullData = sc.read().csv("data/kc_house_data.csv");
        System.out.println(strRowRdd.count());
        JavaRDD arr = strRowRdd.flatMap(x -> Arrays.asList(x.split(",")).iterator());
        List<String> l = arr.take(3);
        for (String object : l) {
            System.out.println(object);
            System.out.println("--------------------");
        }
        JavaRDD<String> filteredRows = strRowRdd.filter(s -> s.contains("Santa Barbara"));
        System.out.println("filteredRows.count " + filteredRows.count());
        System.out.println("filteredRows " + filteredRows.take(1));
        JavaRDD<Integer> rowlengths = strRowRdd.map(s -> s.length());
        /*List<Integer> rows = rowlengths.collect();
        for (Integer row : rows) {
            System.out.println(row);
        }*/


    }

}
