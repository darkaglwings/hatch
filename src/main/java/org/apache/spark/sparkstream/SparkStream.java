package org.apache.spark.sparkstream;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public class SparkStream {

	public static Queue<JavaRDD<Object>> queue = new LinkedBlockingQueue<JavaRDD<Object>>();
	
	String master = "spark://hdpdev-as00017:7077";
	
	public void stream() {
		SparkConf conf = new SparkConf().setMaster(master).setAppName("stream");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		//JavaDStream<Object> lines = jssc.;
		
		//lines.print();
	}
	
	public static void main(String[] args) {
		double a = 0.09d;
		double b = 0.01d;
		
		System.out.println(a+b);
	}
	
}
