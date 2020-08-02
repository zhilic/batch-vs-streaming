package batch;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import config.Conf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;


import java.text.SimpleDateFormat;

import static config.Conf.DATA_PATH;
import static org.apache.spark.sql.functions.*;

public class SparkBatch {

    public static void group1(Dataset<Row> df) {
        /** Question 1.1 & 3.1 */
        Dataset<Row> originAirport = df.groupBy("Origin").count()
                .withColumnRenamed("Origin", "Airport");
        Dataset<Row> destAirport = df.groupBy("Dest").count()
                .withColumnRenamed("Dest", "Airport");
        Dataset<Row> allAirports = originAirport.union(destAirport)
                .groupBy("Airport")
                .sum("count")
                .withColumnRenamed("sum(count)", "popularity")
                .withColumn("rank", row_number().over(Window.orderBy(desc("popularity"))))
                .limit(10)
                ;

        allAirports.write().format("org.apache.spark.sql.cassandra")
                .option("keyspace", Conf.KEYSPACE_BATCH)
                .option("table", Conf.TABLE11)
                .save();
//        allAirports.show();

        /** Question 1.2 */
        Dataset<Row> airlinesOnTime = df.groupBy("UniqueCarrier")
                .avg("ArrDelay")
                .withColumnRenamed("avg(ArrDelay)", "avgarrdelay")
                .withColumn("avgarrdelay", round(col("avgarrdelay"), 2))
                .withColumn("rank", row_number().over(Window.orderBy("avgarrdelay")))
                .limit(10)
                ;

        airlinesOnTime.write().format("org.apache.spark.sql.cassandra")
                .option("keyspace", Conf.KEYSPACE_BATCH)
                .option("table", Conf.TABLE12)
                .save();
    }

    /** Question 2.1 */
    public static void question21(Dataset<Row> df) {

        Dataset<Row> onTimeDepByCarrier = df.groupBy("Origin", "UniqueCarrier")
                .agg(avg("DepDelay").as("avgdepdelay"))
                .na().drop()
                .withColumn("top", row_number().over(Window.partitionBy("Origin").orderBy("avgdepdelay")))
                .filter(col("top").$less(11))
                .orderBy("Origin", "avgdepdelay")
                .withColumn("avgdepdelay", round(col("avgdepdelay"), 2))
                .withColumn("top10", concat_ws(", ", col("UniqueCarrier"), col("avgdepdelay")))
                .groupBy("Origin")
                .agg(collect_list("top10").as("top"))
                .withColumn("topcarrier", col("top").cast(DataTypes.StringType))
                .drop("top")
                .withColumnRenamed("Origin", "airport")
                ;

        onTimeDepByCarrier.write().format("org.apache.spark.sql.cassandra")
                .option("keyspace", Conf.KEYSPACE_BATCH)
                .option("table", Conf.TABLE21)
                .save();
    }

    /** Question 2.2 */
    public static void question22(Dataset<Row> df) {

        Dataset<Row> onTimeDepByDest = df.groupBy("Origin", "Dest")
                .agg(avg("DepDelay").as("avgdepdelay"))
                .na().drop()
                .withColumn("top", row_number().over(Window.partitionBy("Origin").orderBy("avgdepdelay")))
                .filter(col("top").$less(11))
                .orderBy("Origin", "avgdepdelay")
                .withColumn("avgdepdelay", round(col("avgdepdelay"), 2))
                .withColumn("top10", concat_ws(", ", col("Dest"), col("avgdepdelay")))
                .groupBy("Origin")
                .agg(collect_list("top10").as("top"))
                .withColumn("topdest", col("top").cast(DataTypes.StringType))
                .drop("top")
                .withColumnRenamed("Origin", "airport");

        onTimeDepByDest.write().format("org.apache.spark.sql.cassandra")
                .option("keyspace", Conf.KEYSPACE_BATCH)
                .option("table", Conf.TABLE22)
                .save();
    }

    /** Question 2.4 */
    public static void question24(Dataset<Row> df) {

        Dataset<Row> onTimeArrival = df.groupBy("Origin", "Dest")
                .agg(avg("ArrDelay").as("arrdelay"))
                .na().drop()
                .withColumnRenamed("Origin", "origin")
                .withColumnRenamed("Dest", "dest")
                .withColumn("arrdelay", round(col("arrdelay"), 2));

        onTimeArrival.write().format("org.apache.spark.sql.cassandra")
                .option("keyspace", Conf.KEYSPACE_BATCH)
                .option("table", Conf.TABLE24)
                .save();
    }

    /** Question 3.2 (all possible combinations) */
    public static void quesiton32(Dataset<Row> df) {

        Dataset<Row> leg1 = df.filter(col("Year").equalTo(2008))
                .filter(col("CRSDepTime").$less(1200))
                .na().drop()
                .withColumn("minDelay", row_number().over(
                        Window.partitionBy("Origin", "Dest", "FlightDate").orderBy("ArrDelay")))
                .filter(col("minDelay").$less(2))
                .withColumnRenamed("Origin", "airportx")
                .withColumnRenamed("Dest", "dest1")
                .withColumnRenamed("FlightDate", "flightdate11")
                .withColumnRenamed("UniqueCarrier", "uniquecarrier1")
                .withColumnRenamed("FlightNum", "flightnum1")
                .withColumnRenamed("CRSDepTime", "crsdeptime1")
                .withColumnRenamed("ArrDelay", "arrdelay1")
                .drop("minDelay", "Year", "DayOfWeek", "DepDelay")
                ;

        Dataset<Row> leg2 = df.filter(col("Year").equalTo(2008))
                .filter(col("CRSDepTime").$greater(1200))
                .na().drop()
                .withColumn("minDelay", row_number().over(
                        Window.partitionBy("Origin", "Dest", "FlightDate").orderBy("ArrDelay")))
                .filter(col("minDelay").$less(2))
                .withColumnRenamed("Origin", "origin2")
                .withColumnRenamed("Dest", "airportz")
                .withColumnRenamed("FlightDate", "flightdate22")
                .withColumnRenamed("UniqueCarrier", "uniquecarrier2")
                .withColumnRenamed("FlightNum", "flightnum2")
                .withColumnRenamed("CRSDepTime", "crsdeptime2")
                .withColumnRenamed("ArrDelay", "arrdelay2")
                .drop("minDelay", "Year", "DayOfWeek", "DepDelay")
                ;

        Dataset<Row> q32 = leg1.join(leg2, leg1.col("dest1").equalTo(leg2.col("origin2")))
                .where(leg1.col("flightdate11").equalTo(date_sub(leg2.col("flightdate22"), 2)))
                .withColumn("totaldelay", col("arrdelay1").plus(col("arrdelay2")))
                .withColumn("flightdate1", date_format(col("flightdate11"), "dd/MM/yyyy"))
                .withColumn("flightdate2", date_format(col("flightdate22"), "dd/MM/yyyy"))
                .withColumnRenamed("dest1", "airporty")
                .drop("origin2", "flightdate11", "flightdate22")
                ;

        q32.write().format("org.apache.spark.sql.cassandra")
                .option("keyspace", Conf.KEYSPACE_BATCH)
                .option("table", Conf.TABLE32)
                .save();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Batch Processing")
//                .config("spark.cassandra.connection.host", "54.144.142.192")
//                .config("spark.cassandra.connection.port", "9042")
                .master("local[*]")
                .getOrCreate();

        SparkContext sc = spark.sparkContext();
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load(DATA_PATH + "*.csv")
                ;
        df = df.withColumn("Year", df.col("Year").cast(DataTypes.IntegerType))
                .withColumn("DayOfWeek", df.col("DayOfWeek").cast(DataTypes.IntegerType))
                .withColumn("FlightDate", df.col("FlightDate").cast(DataTypes.DateType))
                .withColumn("UniqueCarrier", df.col("UniqueCarrier").cast(DataTypes.StringType))
                .withColumn("FlightNum", df.col("FlightNum").cast(DataTypes.StringType))
                .withColumn("Origin", df.col("Origin").cast(DataTypes.StringType))
                .withColumn("Dest", df.col("Dest").cast(DataTypes.StringType))
                .withColumn("CRSDepTime", df.col("CRSDepTime").cast(DataTypes.IntegerType))
                .withColumn("DepDelay", df.col("DepDelay").cast(DataTypes.FloatType))
                .withColumn("ArrDelay", df.col("ArrDelay").cast(DataTypes.FloatType))
                ;

        group1(df);
        question21(df);
        question22(df);
        question24(df);
        quesiton32(df);

        spark.close();

    }

}

