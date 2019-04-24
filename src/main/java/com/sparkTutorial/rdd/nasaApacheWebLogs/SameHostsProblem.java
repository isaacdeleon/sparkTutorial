package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf conf = new SparkConf().setAppName("unionhostlogs").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julyLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustLogs = sc.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> unionLogs = julyLogs.intersection(augustLogs);

        JavaRDD<String> hostLogsLines = unionLogs.filter(line -> isHost(line));

        JavaRDD<String> onlyhost = hostLogsLines.map(line -> {
            String[] splits = line.split("\t");
            return splits[0];
        });

        onlyhost.saveAsTextFile("out/nasa_logs_same_hosts_problem.csv");
    }

    private static boolean isHost(String line) {
        return (line.startsWith("host"));
    }
}
