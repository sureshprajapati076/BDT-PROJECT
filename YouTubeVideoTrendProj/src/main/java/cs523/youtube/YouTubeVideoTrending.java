package cs523.youtube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class YouTubeVideoTrending 
{
  

  public static void main(String[] args) throws InterruptedException, IOException 
  {
	  
	  
	  HbaseTable.CreateTable();
	  
	// MyFirstHbaseTable.addData(new ArrayList<String>());
	  
	  
	  JavaStreamingContext ssc = new JavaStreamingContext(new SparkConf().setAppName("wordCount").setMaster("local"), Durations.seconds(15));
	  
	  
	  JavaDStream<String> lines = ssc.textFileStream("hdfs://localhost/user/cloudera/test");
	
	 
		  
		  
		  
		  
		  
		  
		  
		  
		  
	  
	 JavaDStream<List<String>> counter = lines.map(line->Arrays.asList(line.split("\\t")));
	  
	  
	  
	  
	  counter.foreachRDD(rdd ->{
		  
		  
		  if(!rdd.isEmpty()) {
		  System.out.println("Contents of added File are:\n");
		  
		  
		  rdd.collect().forEach(x->{
			  
			  
			 // System.out.println(x);
			  
			  try {
				HbaseTable.addData(x);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			  
		  
		  
		  
		  });
		  rdd.coalesce(1).saveAsTextFile("/home/cloudera/output");
		  
		  }
      });
	  
	  
	  
	  
	  
	  
	  
	  ssc.start();
	  
	 

	  
	  
	  
	  
	  
	  ssc.awaitTermination();
	  
	  
	  

	  
	  
	 
	 
  }
}
