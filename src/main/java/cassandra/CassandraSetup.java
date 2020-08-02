package cassandra;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import config.Conf;
import org.apache.spark.SparkConf;

public class CassandraSetup {

    private SparkConf sc;

    public CassandraSetup(SparkConf sc, String keyspace) {
        this.sc = sc;
        CassandraConnector connector = CassandraConnector.apply(sc);

        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
            session.execute("CREATE KEYSPACE " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1}");
            session.execute("USE " + keyspace);
            session.execute("DROP TABLE IF EXISTS " + Conf.TABLE11);
            session.execute("CREATE TABLE  " + Conf.TABLE11 + "(rank bigint PRIMARY KEY, airport text, popularity int)");
            session.execute("DROP TABLE IF EXISTS " + Conf.TABLE12);
            session.execute("CREATE TABLE  " + Conf.TABLE12 + "(rank bigint PRIMARY KEY, airline text, avgarrdelay float)");
            session.execute("DROP TABLE IF EXISTS " + Conf.TABLE21);
            session.execute("CREATE TABLE  " + Conf.TABLE21 + "(airport text PRIMARY KEY, topcarrier text)");
            session.execute("DROP TABLE IF EXISTS " + Conf.TABLE22);
            session.execute("CREATE TABLE  " + Conf.TABLE22 + "(airport text PRIMARY KEY, topdest text)");
            session.execute("DROP TABLE IF EXISTS " + Conf.TABLE24);
            session.execute("CREATE TABLE  " + Conf.TABLE24 + "(origin text, dest text, arrdelay double, PRIMARY KEY (origin, dest))");
            session.execute("DROP TABLE IF EXISTS " + Conf.TABLE32);
            session.execute("CREATE TABLE  " + Conf.TABLE32 + "(airportx text, airporty text, airportz text, FlightDate1 text, " +
                    "UniqueCarrier1 text, FlightNum1 text, CRSDepTime1 int, ArrDelay1 float, FlightDate2 text, " +
                    "UniqueCarrier2 text, FlightNum2 text, CRSDepTime2 int, ArrDelay2 float, TotalDelay float, " +
                    "PRIMARY KEY (FlightDate1, airportx, airporty, airportz))");
        }
    }

}

