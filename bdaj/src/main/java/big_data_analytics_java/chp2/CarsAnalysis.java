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

        System.out.println("---------- select * from car ----------");
        carsBaseDF.createOrReplaceTempView("cars");
        Dataset<Row> netDF = spark.sql("select * from cars");
        netDF.show ();
        System.out.println("Total Rows in Data = " + netDF.count());

        System.out.println("---------- where make_country 'Italy' ----------");
        Dataset<Row> italyCarsDF = spark.sql("select * from cars where make_country = 'Italy'");
        italyCarsDF.show(); //show the full content
        //italyCarsDF.write().format("json").save("tmp/cars/italycars.json"); lack of rights

        System.out.println("italyCarsDF count = " + italyCarsDF.toJavaRDD().count());

        Dataset<Row> distinctCntryDF = spark.sql("select distinct make_country from cars");
        distinctCntryDF.show();

    }

}
