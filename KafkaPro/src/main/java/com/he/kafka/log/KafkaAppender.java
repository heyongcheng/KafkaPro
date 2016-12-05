package com.he.kafka.log;

import com.he.kafka.KafkaProducer;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class KafkaAppender extends AppenderBase<ILoggingEvent>{

	private String topic;
	private Formatter formatter;
	private KafkaProducer producer;
	
	@Override
	protected void append(ILoggingEvent event) {
		String message = this.formatter.format(event);
		if(this.producer != null){
			producer.send(message);
		}
			
	}

	@Override
	public void start() {
		if(this.formatter == null){
			this.formatter = new MessageFormatter();
		}
		super.start();
		this.producer = new KafkaProducer(topic);
	}
	
	@Override
	public void stop() {
		super.stop();
	}


	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
}
