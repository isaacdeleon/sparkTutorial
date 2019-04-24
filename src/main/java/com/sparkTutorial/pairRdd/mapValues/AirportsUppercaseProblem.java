package com.sparkTutorial.pairRdd.mapValues;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AirportsUppercaseProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
           being the key and country name being the value. Then convert the country name to uppercase and
           output the pair RDD to out/airports_uppercase.text

           Each row of the input file contains the following columns:

           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "CANADA")
           ("Wewak Intl", "PAPUA NEW GUINEA")
           ...
         */
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("airportsRdd").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportsRDD = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> airportsPair = airportsRDD.mapToPair(getAirportNameAndCountryNamePair());

        JavaPairRDD<String, String> airPortUpperCase = airportsPair.mapValues(countryName -> countryName.toUpperCase());

        airPortUpperCase.saveAsTextFile("out/airports_uppercase.text");

    }

    private static PairFunction<String, String, String> getAirportNameAndCountryNamePair() {
        return (PairFunction<String, String, String>)
                line ->
                        new Tuple2<>(
                                line.split(Utils.COMMA_DELIMITER)[1], line.split(Utils.COMMA_DELIMITER)[3]);
    }
}
