package streaming;

import config.Conf;
import cassandra.CassandraSetup;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.*;

import java.lang.Float;
import java.lang.Long;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SparkStreaming {

    /* Question 3.2 Queries */
    private static Set<String> airportX = new HashSet<>(Arrays.asList("BOS", "PHX", "DFW", "LAX", "CMH"));
    private static Set<String> airportY = new HashSet<>(Arrays.asList("ATL", "JFK", "STL", "MIA", "STL"));
    private static Set<String> airportZ = new HashSet<>(Arrays.asList("LAX", "MSP", "ORD", "LAX", "SNA"));
    private static Set<String> dates1 =
            new HashSet<>(Arrays.asList("03/04/2008", "07/09/2008", "24/01/2008", "16/05/2008", "01/07/1990"));  //dd/mm/yyyy

    private static Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateSumFunction =
            (newValues, state) -> {
                Integer newSum = 0;
                if (state.isPresent()) {newSum = state.get();}
                for (Integer newValue : newValues) { newSum += newValue;}
                return Optional.of(newSum);
            };

    private static Function2<List<Tuple3<Float, Integer, Float>>, Optional<Tuple3<Float, Integer, Float>>,
            Optional<Tuple3<Float, Integer, Float>>> updateAvgFunction =
            (newValues, state) -> {
                Float newSum = 0.0f;
                Integer newCount = 0;
                Float newAvg = 0.0f;
                if (state.isPresent()) {
                    newSum = state.get()._1();
                    newCount = state.get()._2();
                    newAvg = state.get()._3();
                }
                for (Tuple3<Float, Integer, Float> newValue : newValues) {
                    newSum += newValue._1();
                    newCount += newValue._2();
                    newAvg = newSum / newCount;
                }
                return Optional.of(new Tuple3<Float, Integer, Float>(newSum, newCount, newAvg));
            };

    /** Q3.2: Update the minimum ArrDelay (with UniqueCarrier, FlightNum, and CRSDepTime)
     *  for each <Origin, Dest, FlightDate> combination */
    private static Function2<List<Tuple4<String, String, Integer, Float>>,
            Optional<Tuple4<String, String, Integer, Float>>,
            Optional<Tuple4<String, String, Integer, Float>>> updateMinFunction =
            (newValues, state) -> {
                String newUC = null; String newFN = null; Integer newDT = null;
                Float newMin = Float.POSITIVE_INFINITY;
                if (state.isPresent()) {
                    newUC = state.get()._1(); newFN = state.get()._2();
                    newDT = state.get()._3(); newMin = state.get()._4();
                }
                for (Tuple4<String, String, Integer, Float> newValue : newValues) {
                    if (newValue._4() < newMin) {
                        newUC = newValue._1(); newFN = newValue._2();
                        newDT = newValue._3(); newMin = newValue._4();
                    }
                }
                return Optional.of(new Tuple4<String, String, Integer, Float>(newUC, newFN, newDT, newMin));
            };

    private static Integer getColIndex(String colName) {
        switch (colName) {
            case "Year": return 0;
            case "DayOfWeek": return 1;
            case "FlightDate": return 2;
            case "UniqueCarrier": return 3;
            case "FlightNum": return 4;
            case "Origin": return 5;
            case "Dest": return 6;
            case "CRSDepTime": return 7;
            case "DepDelay": return 8;
        }
        return 9;
    }

    private static String getTopString(Iterable<Tuple2<String, Float>> iterable, Integer n) {
        List<Tuple2<String, Float>> list = StreamSupport
                .stream(iterable.spliterator(), false)
                .sorted(Comparator.comparing(Tuple2::_2))
                .limit(n)
                .collect(Collectors.toList());
        return list.toString();
    }

    /** Question 1.1
     *  Rank the top 10 most popular airports by numbers of flights to/from the airport. */
    private static void question11(JavaDStream<List<String>> rows) {
        JavaPairDStream<String, Integer> originCounts = rows.mapToPair(row ->
                new Tuple2<>(row.get(getColIndex("Origin")), 1));
        JavaPairDStream<String, Integer> destCounts = rows.mapToPair(row ->
                new Tuple2<>(row.get(getColIndex("Dest")), 1));

        JavaPairDStream<String, Integer> airportPopularity = originCounts.union(destCounts)
                .reduceByKey((r1, r2) -> r1 + r2)
                .updateStateByKey(updateSumFunction);

        JavaDStream<Tuple3<Long, String, Integer>> results = airportPopularity
                .mapToPair(record -> record.swap())
                .transform(rdd -> {
                    List<Tuple2<Integer, String>> top10 = rdd.sortByKey(false).take(10);
                    rdd = rdd.filter(record -> top10.contains(record)).sortByKey(false);
                    JavaRDD<Tuple3<Long, String, Integer>> newRDD = rdd
                            .zipWithIndex()    // JavaPairRDD<T, Long>
                            .map(record -> new Tuple3<>(record._2() + 1, record._1()._2(), record._1()._1()));  // +1 to make index starting from 1
                    return newRDD;
                });

        results.print();
        CassandraStreamingJavaUtil.javaFunctions(results)
                .writerBuilder(Conf.KEYSPACE_STREAMING, Conf.TABLE11,
                        CassandraJavaUtil.mapTupleToRow(Long.class, String.class, Integer.class))
                .withColumnSelector(CassandraJavaUtil.someColumns("rank", "airport", "popularity"))
                .saveToCassandra();
    }

    /** Question 1.2
     *  Rank the top 10 airlines by on-time arrival performance. */
    private static void question12(JavaDStream<List<String>> rows) {
        // if row.size <= 9, ArrDelay is a missing value, then pass this message
        JavaDStream<List<String>> validArrDelay = rows.filter(row -> row.size() > 9);
        JavaPairDStream<String, Tuple3<Float, Integer, Float>> avgArrDelay = validArrDelay
                .mapToPair(row -> {
                    Float f = Float.parseFloat(row.get(getColIndex("ArrDelay")));
                    return new Tuple2<>(row.get(getColIndex("UniqueCarrier")),
                            new Tuple3<>(f, 1, f));
                })
                .reduceByKey((r1, r2) -> new Tuple3<>(r1._1() + r2._1(), r1._2() + r2._2(),
                        (r1._1() + r2._1()) / (r1._2() + r2._2())))
                .updateStateByKey(updateAvgFunction);

        JavaDStream<Tuple3<Long, String, Float>> results = avgArrDelay
                .mapToPair(record -> new Tuple2<>(record._2()._3(), record._1()))   // Tuple2<Float, String>
                .transform(rdd -> {
                    List<Tuple2<Float, String>> top10 = rdd.sortByKey(true).take(10);
                    rdd = rdd.filter(record -> top10.contains(record)).sortByKey(true);
                    JavaRDD<Tuple3<Long, String, Float>> newRDD = rdd
                            .zipWithIndex()
                            .map(record -> new Tuple3<>(record._2() + 1, record._1()._2(), record._1()._1()));
                    return newRDD;
                });

        results.print();
        CassandraStreamingJavaUtil.javaFunctions(results)
                .writerBuilder(Conf.KEYSPACE_STREAMING, Conf.TABLE12,
                        CassandraJavaUtil.mapTupleToRow(Long.class, String.class, Float.class))
                .withColumnSelector(CassandraJavaUtil.someColumns("rank", "airline", "avgarrdelay"))
                .saveToCassandra();
    }

    /** Question 2.1 & 2.2
     *  For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.
     *  For each source airport X, rank the top-10 destination airports in decreasing order of on-time departure performance from X.
     *  */
    private static void question2122(JavaDStream<List<String>> rows, String field,
                                     String cassandraTableName, String cassandraColName) {
        JavaDStream<List<String>> validDepDelay = rows.filter(row -> row.size() > 8);

        JavaPairDStream<Tuple2<String, String>, Tuple3<Float, Integer, Float>> avgDepDelay =
                validDepDelay
                        .mapToPair(row -> {
                            Float f = Float.parseFloat(row.get(getColIndex("DepDelay")));
                            return new Tuple2<>(
                                    new Tuple2<>(row.get(getColIndex("Origin")),
                                            row.get(getColIndex(field))),
                                    new Tuple3<>(f, 1, f));
                        })
                        .reduceByKey((r1, r2) -> new Tuple3<>(r1._1() + r2._1(), r1._2() + r2._2(),
                                (r1._1() + r2._1()) / (r1._2() + r2._2())))
                        .updateStateByKey(updateAvgFunction);

        JavaPairDStream<String, Iterable<Tuple2<String, Float>>> avgDepDelayByAirline = avgDepDelay
                .mapToPair(record -> new Tuple2<>(record._1()._1(),
                        new Tuple2<>(record._1()._2(), record._2()._3())))  // <airport, <dest/airline, avgArrDelay>>
                .groupByKey();    // <airport, Iterable<dest/airline, avgArrDelay>>

        JavaDStream<Tuple2<String, String>> results = avgDepDelayByAirline
                .map(record -> new Tuple2<>(record._1(), getTopString(record._2(), 10)));
        results.print();

        CassandraStreamingJavaUtil.javaFunctions(results)
                .writerBuilder(Conf.KEYSPACE_STREAMING, cassandraTableName,
                        CassandraJavaUtil.mapTupleToRow(String.class, String.class))
                .withColumnSelector(CassandraJavaUtil.someColumns("airport", cassandraColName))
                .saveToCassandra();
    }

    /** Question 2.4
     * For each source-destination pair X-Y, determine the mean arrival delay (in minutes) for a flight from X to Y. */
    private static void question24(JavaDStream<List<String>> rows) {
        JavaDStream<List<String>> validArrDelay = rows.filter(row -> row.size() > 9);
        JavaDStream<Tuple3<String, String, Float>> avgArrDelay = validArrDelay
                .mapToPair(row -> {
                    Float f = Float.parseFloat(row.get(getColIndex("ArrDelay")));
                    return new Tuple2<>(
                            new Tuple2<>(row.get(getColIndex("Origin")),
                                    row.get(getColIndex("Dest"))),
                            new Tuple3<>(f, 1, f));
                })
                .reduceByKey((r1, r2) -> new Tuple3<>(r1._1() + r2._1(), r1._2() + r2._2(),
                        (r1._1() + r2._1()) / (r1._2() + r2._2())))
                .updateStateByKey(updateAvgFunction)
                .map(record -> new Tuple3<>(record._1()._1(), record._1()._2(), record._2()._3()));
//        results.print();

        CassandraStreamingJavaUtil.javaFunctions(avgArrDelay)
                .writerBuilder(Conf.KEYSPACE_STREAMING, Conf.TABLE24,
                        CassandraJavaUtil.mapTupleToRow(String.class, String.class, Float.class))
                .withColumnSelector(CassandraJavaUtil.someColumns("origin", "dest", "arrdelay"))
                .saveToCassandra();
    }

    /** Question 3.2 */
    private static void question32(JavaDStream<List<String>> rows) {

        // SimpleDateFormat is not thread safe, so better to make it local
        SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

        /** Generate corresponding dates for leg 2 */
        Set<Date> leg1Dates = new HashSet<>();
        Set<Date> leg2Dates = new HashSet<>();
        for (String date : dates1) {
            try {
                Date leg1Date = sdf1.parse(date);
                leg1Dates.add(leg1Date);
                Date leg2Date = new Date(leg1Date.getTime() + 2 * 24 * 60 * 60 * 1000);
                leg2Dates.add(leg2Date);
            } catch (ParseException e) {e.printStackTrace();}
        }

        JavaDStream<List<String>> valid = rows
                .filter(row -> row.size() > 9)
                .filter(row -> row.get(getColIndex("Year")).equals("2008"));

        /** Find the satisfying flights for leg 1 */
        JavaDStream<List<String>> validLeg1 = valid
                .filter(row -> airportX.contains(row.get(getColIndex("Origin"))))
                .filter(row -> airportY.contains(row.get(getColIndex("Dest"))))
                .filter(row -> {
                    Date flightDate = sdf2.parse(row.get(getColIndex("FlightDate")));
                    return leg1Dates.contains(flightDate);
                })
                .filter(row -> Integer.parseInt(row.get(getColIndex("CRSDepTime"))) < 1200);
        JavaPairDStream<Tuple3<String, String, Date>, Tuple4<String, String, Integer, Float>> leg1 =
                q32MapToPair(validLeg1)
                        .reduceByKey((r1, r2) -> r1._4() < r2._4() ? r1 : r2)
                        .updateStateByKey(updateMinFunction);
        JavaPairDStream<Tuple2<String, Date>, Tuple6<String, Date, String, String, Integer, Float>> leg1ToJoin =
                leg1.mapToPair(row -> new Tuple2<>(
                        new Tuple2<>(row._1()._2(), row._1()._3()),   // airportY, FlightDate1
                        new Tuple6<>(row._1()._1(), row._1()._3(), row._2()._1(),
                                row._2()._2(), row._2()._3(), row._2()._4())));

        /** Find the satisfying flights for leg 2 */
        JavaDStream<List<String>> validLeg2 = valid
                .filter(row -> airportY.contains(row.get(getColIndex("Origin"))))
                .filter(row -> airportZ.contains(row.get(getColIndex("Dest"))))
                .filter(row -> {
                    Date flightDate = sdf2.parse(row.get(getColIndex("FlightDate")));
                    return leg2Dates.contains(flightDate);
                })
                .filter(row -> Integer.parseInt(row.get(getColIndex("CRSDepTime"))) > 1200);
        JavaPairDStream<Tuple3<String, String, Date>, Tuple4<String, String, Integer, Float>> leg2 =
                q32MapToPair(validLeg2)
                        .reduceByKey((r1, r2) -> r1._4() < r2._4() ? r1 : r2)
                        .updateStateByKey(updateMinFunction);

        JavaPairDStream<Tuple2<String, Date>, Tuple6<String, Date, String, String, Integer, Float>> leg2ToJoin =
                leg2.mapToPair(row -> new Tuple2<>(
                        new Tuple2<>(row._1()._1(), new Date(row._1()._3().getTime() - 2 * 24 * 60 * 60 * 1000)),    // airportY, FlightDate1
                        new Tuple6<>(row._1()._2(), row._1()._3(), row._2()._1(),
                                row._2()._2(), row._2()._3(), row._2()._4())));

        /** <airportY, FlightDate1>,
         *  Tuple2<Optional<Tuple6<airportX, FlightDate1, UniqueCarrier1, FlightNum1, CRSDepTime1, ArrDelay1>>,
         *         Optional<Tuple6<airportZ, FlightDate2, UniqueCarrier2, FlightNum2, CRSDepTime2, ArrDelay2>>> */
        JavaPairDStream<Tuple2<String, Date>, Tuple2<Optional<Tuple6<String, Date, String, String, Integer, Float>>,
                Optional<Tuple6<String, Date, String, String, Integer, Float>>>> joined =
                leg1ToJoin.fullOuterJoin(leg2ToJoin);
        JavaDStream<Tuple14<String, String, String, String, String, String, Integer, Float,
                String, String, String, Integer, Float, Float>> results =
                joined.map(row -> {
                    Tuple6<String, Date, String, String, Integer, Float> leg1Info = row._2._1.get();
                    Tuple6<String, Date, String, String, Integer, Float> leg2Info = row._2._2.get();
                    return new Tuple14<>(leg1Info._1(), row._1()._1(), leg2Info._1(), sdf1.format(leg1Info._2()),
                            leg1Info._3(), leg1Info._4(), leg1Info._5(), leg1Info._6(), sdf1.format(leg2Info._2()),
                            leg2Info._3(), leg2Info._4(), leg2Info._5(), leg2Info._6(), leg1Info._6() + leg2Info._6());
                });

        results.print();

        CassandraStreamingJavaUtil.javaFunctions(results)
                .writerBuilder(Conf.KEYSPACE_STREAMING, Conf.TABLE32,
                        CassandraJavaUtil.mapTupleToRow(String.class, String.class, String.class, String.class,
                                String.class, String.class, Integer.class, Float.class, String.class, String.class,
                                String.class, Integer.class, Float.class, Float.class))
                .withColumnSelector(CassandraJavaUtil.someColumns("airportx", "airporty", "airportz", "flightdate1",
                        "uniquecarrier1", "flightnum1", "crsdeptime1", "arrdelay1", "flightdate2",
                        "uniquecarrier2", "flightnum2", "crsdeptime2", "arrdelay2", "totaldelay"))
                .saveToCassandra();
    }

    private static JavaPairDStream<Tuple3<String, String, Date>, Tuple4<String, String, Integer, Float>>
    q32MapToPair(JavaDStream<List<String>> rows) {
        // SimpleDateFormat is not thread safe, so better to make it local
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

        JavaPairDStream<Tuple3<String, String, Date>, Tuple4<String, String, Integer, Float>> results =
                rows.mapToPair(row -> new Tuple2<>(
                        new Tuple3<>(row.get(getColIndex("Origin")),
                                row.get(getColIndex("Dest")),
                                sdf2.parse(row.get(getColIndex("FlightDate")))),
                        new Tuple4<>(row.get(getColIndex("UniqueCarrier")),
                                row.get(getColIndex("FlightNum")),
                                Integer.parseInt(row.get(getColIndex("CRSDepTime"))),
                                Float.parseFloat(row.get(getColIndex("ArrDelay"))))));
        return results;
    }


    public static void main(String[] args) throws InterruptedException{

        Collection<String> topics = Arrays.asList(Conf.TOPIC);  // could consume multiple topics

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkStreamingApp")
//                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", Conf.CASSANDRA_HOST)
                .set("spark.cassandra.connection.port", Conf.CASSANDRA_PORT)
                .set("spark.cassandra.connection.keep_alive_ms", "10000")
                .set("spark.streaming.kafka.maxRatePerPartition", "1000000");

        CassandraSetup cassandra = new CassandraSetup(sparkConf, Conf.KEYSPACE_STREAMING);

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        streamingContext.checkpoint(Conf.CHECKPOINT_DIR);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                streamingContext, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, Conf.KAFKA_PARAMS));

        JavaDStream<List<String>> rows = messages.map(record -> Arrays.asList(record.value().split(",")));

        question11(rows);
        question12(rows);
        question2122(rows, "UniqueCarrier", Conf.TABLE21, "topcarrier");
        question2122(rows, "Dest", Conf.TABLE22, "topdest");
        question24(rows);
        question32(rows);

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}

