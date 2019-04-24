package com.sparkTutorial.pairRdd.groupbykey;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class AirportsByCountryProblem {

  public static void main(String[] args) throws Exception {

    /* Create a Spark program to read the airport data from in/airports.text,
      output the the list of the names of the airports located in each country.

      Each row of the input file contains the following columns:
      Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
      ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

      Sample output:

      "Canada", ["Bagotville", "Montreal", "Coronation", ...]
      "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
      "Papua New Guinea",  ["Goroka", "Madang", ...]
      ...
    */
    SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> lines = sc.textFile("in/airports.text");

    JavaPairRDD<String, String> countryAndAirportNamePair =
        lines.mapToPair(
            airports ->
                new Tuple2<>(
                    airports.split(Utils.COMMA_DELIMITER)[3],
                    airports.split(Utils.COMMA_DELIMITER)[1]));

    JavaPairRDD<String, Iterable<String>> airportsByCountry =
        countryAndAirportNamePair.groupByKey();

    for (Map.Entry<String, Iterable<String>> airports :
        airportsByCountry.collectAsMap().entrySet()) {
      System.out.println(airports.getKey() + " : " + airports.getValue());
    }
  }
}
