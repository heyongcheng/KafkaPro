package com.he.kafka;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder; 

public class KafkaProducer implements Runnable{
	
	private static Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);
	
	private String topic;
	private Producer producer;
	
	public KafkaProducer(String topic){
		this.topic = topic;
		this.producer = createProducer();
	}
	
	public void run() {
		int i = 0;
		while(i++ < 10){
			producer.send(new KeyedMessage<Integer, String>(topic, "message:" + i));
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	
	private Producer createProducer() {
		Properties prop = new Properties();
		prop.put("zookeeper.connect", "192.168.8.120:2181");
		prop.put("serializer.class", StringEncoder.class.getName());
		prop.put("metadata.broker.list", "192.168.8.120:9092");
		return new Producer<Integer, String>(new ProducerConfig(prop));
	}
	
	public void send(String message){
		producer.send(new KeyedMessage<Integer, String>(topic, message));
	}
	
	public static void main(String[] args) {
		/*Thread thread = new Thread(new KafkaProducer("test"));
		thread.start();*/
		for(int i=0;i<1000;i++){
			LOG.info("KAFKA APPENDER:" + i);	
		}
	}
	
}
