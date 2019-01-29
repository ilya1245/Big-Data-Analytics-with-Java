package big_data_analytics_java.chp2;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class CarsAnalysis {
    public static void main(String[] args) {
        LogManager.getLogger("org").setLevel(Level.OFF);

        SparkConf c = new SparkConf()
                .setMaster("local[*]")
                .setAppName("CarsAnalysis")
                .set("spark.driver.allowMultipleContexts", "true");

        SparkSession spark = SparkSession
                .builder()
                .config(c)
                .appName("CarsAnalysis")
                .getOrCreate();

        System.out.println("---------- carsBaseDF ----------");
        Dataset<Row> carsBaseDF = spark.read().json("resources/data/cars/cars.json");
        carsBaseDF.show();

        System.out.println("---------- carsBaseDF1 ----------");
        //SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(c);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        Dataset<Row> carsBaseDF1 = sqlContext.read().json("resources/data/cars/cars.json");
        carsBaseDF1.show();
    }

}
