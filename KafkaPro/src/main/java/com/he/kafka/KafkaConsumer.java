package com.he.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class KafkaConsumer implements Runnable{
	
	private String topic;
	
	public KafkaConsumer(String topic){
		this.topic = topic;
	}

	public void run() {
			
		ConsumerConnector consumer = createConsumer();
		Map<String,Integer> topicCountMap = new HashMap<String,Integer>();
		topicCountMap.put(topic, 1);
		Map<String,List<KafkaStream<byte[],byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		while(iterator.hasNext()){
			String message = new String(iterator.next().message());
			System.out.println("接收到:" + message);
		}
	}
	
	
	
	private ConsumerConnector createConsumer() {
		Properties prop = new Properties();
		prop.put("zookeeper.connect", "192.168.8.120:2181");
		prop.put("group.id", "group1");
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
	}

	public static void main(String[] args) {
		Thread thread = new Thread(new KafkaConsumer("test"));
		thread.start();
	}
}
