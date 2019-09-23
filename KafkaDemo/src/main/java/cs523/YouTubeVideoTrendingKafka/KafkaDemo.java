package cs523.YouTubeVideoTrendingKafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaDemo {
	
	private static String KafkaTopic = null;
	private static String CsvFile = null;
	public static long count=0;
	
	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException{
		HbaseTable.CreateTable();
		
		System.out.println("Starting file "+args[1]);
		
		if (args != null) {
			KafkaTopic = args[0];
			CsvFile = args[1];
		}
		
		KafkaDemo kafkaProducer = new KafkaDemo();
		kafkaProducer.PublishMessage();
		
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
	private void PublishMessage() throws InterruptedException {
		final Producer<String, String> CsvProducer = ProducerProperties();
		while(true){
			
			

			try (Stream<String> FileStream = Files.lines(Paths.get(CsvFile)).skip(count)) {
				
				
				FileStream.forEach(line -> {
					count++;
					final ProducerRecord<String, String> CsvRecord = new ProducerRecord<String, String>(KafkaTopic, UUID.randomUUID().toString(), line);
				
					CsvProducer.send(CsvRecord, (metadata, exception) -> {
						if(metadata != null) {
							System.out.println("CsvData: ->" + CsvRecord.key() + "|" + CsvRecord.value());
							List<String> row = Arrays.asList(CsvRecord.value().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"));
							try {
								HbaseTable.addData(row);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
						} else {
							System.out.println("Error Sending CSV Record -> " + CsvRecord.value());
						}
					});
				});
				
				FileStream.close();
				
				
				

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
