package com.dc.spark.app;


import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

//@SpringBootApplication
public class ApacheSparkBasicApplication {
	public static void main(String[] args) {
		//SpringApplication.run(ApacheSparkBasicApplication.class, args);
		System.setProperty("hadoop.home.dir", "I:\\WIN_UTILS\\");
		execute("I:\\inputfile.txt", "src/main/resources/output_spark/");
	}
	
	public static void execute(String inputPathParam, String outputPathParam) {
		System.out.println(System.getProperty("hadoop.home.dir"));
		String inputPath = inputPathParam;
		String outputPath = outputPathParam;
		FileUtils.deleteQuietly(new File(outputPath));
		FileUtils.deleteQuietly(new File(outputPath));

		JavaSparkContext sc = new JavaSparkContext("local", "sparkwordcount");

		JavaRDD<String> rdd = sc.textFile(inputPath);
		JavaPairRDD<String, Integer> counts = rdd
				.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
				.mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
				.reduceByKey((x, y) -> x + y);

		counts.saveAsTextFile(outputPath);

		sc.close();
	}
}
