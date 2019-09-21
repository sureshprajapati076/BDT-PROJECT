package cs523.YouTubeVideoTrendingKafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

public class KafkaDemo {
	
	private static String KafkaTopic = null;
	private static String CsvFile = null;
	
	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException{
		
		if (args != null) {
			KafkaTopic = args[0];
			CsvFile = args[1];
		}
		
		KafkaDemo kafkaProducer = new KafkaDemo();
		kafkaProducer.PublishMessage();
		
//		startKafka();
//		sparkStream();
//		System.out.println("done tested...");
		
	}
	
	//Producer method is responsible for defining the producer configurations
	private static Producer<String, String> ProducerProperties() {
		Properties properties = new Properties();
		
		//Use the bootstrap server to initial the connection
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaStreamer");
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		return new KafkaProducer<String, String>(properties);
	}
	
	//Publish method to read the rows from csv and write the message
	private void PublishMessage() {
		final Producer<String, String> CsvProducer = ProducerProperties();
		
		try {
			Stream<String> FileStream = Files.lines(Paths.get(CsvFile));
			
			FileStream.forEach(line -> {
				final ProducerRecord<String, String> CsvRecord = new ProducerRecord<String, String>(KafkaTopic, UUID.randomUUID().toString(), line);
			
				CsvProducer.send(CsvRecord, (metadata, exception) -> {
					if(metadata != null) {
						System.out.println("CsvData: ->" + CsvRecord.key() + "|" + CsvRecord.value());
						
						
						
					} else {
						System.out.println("Error Sending CSV Record -> " + CsvRecord.value());
					}
				});
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
//	//Spark Stream method
//	@SuppressWarnings("unchecked")
//	public static void sparkStream() {
//		OffsetRange[] offsetRanges = {
//			//Topic, partition, starting offset, ending offset
//			OffsetRange.create("YTVideos", 0, 0, 100),
//			OffsetRange.create("YTVideos",  1, 0, 100)
//		};
//		
//		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("YouTubeVideoTrending").setMaster("local"));
//		
//        JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
//                sc,
//                (Map<String, Object>) ProducerProperties(),
//                offsetRanges,
//                LocationStrategies.PreferConsistent()
//        );
//	}
//	
//    public static void startKafka() throws ExecutionException, InterruptedException {
//        Properties kProp = new Properties();
//        kProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        kProp.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkaStreamer");
//        kProp.put(ProducerConfig.ACKS_CONFIG, "all");
//        kProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongSerializer");
//        kProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        KafkaProducer<Long, String> streamProducer = new KafkaProducer<Long, String>(kProp);
//        for(int i=0;i<10;i++){
//            ProducerRecord<Long,String> data = new ProducerRecord<Long,String>("kafka-spark","testing  "+System.nanoTime());
//            RecordMetadata rm = streamProducer.send(data).get();
//            System.out.println("Topic:"+rm.topic()+":"+rm.offset());
//        }
//    }

}
