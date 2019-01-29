package big_data_analytics_java.chp2;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CarsAnalysis {
    public static void main(String[] args) {
        LogManager.getLogger("org").setLevel(Level.OFF);

        SparkConf c = new SparkConf().setMaster("local[*]");

        SparkSession spark = SparkSession
                .builder()
                .config(c)
                .appName("LoanDefaultPrediction")
                .getOrCreate();


        Dataset<Row> carsBaseDF = spark.read().json("resources/data/cars/cars.json");
    }

}
