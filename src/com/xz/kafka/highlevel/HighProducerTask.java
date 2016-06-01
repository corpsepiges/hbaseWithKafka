package com.xz.kafka.highlevel;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class HighProducerTask implements Runnable {
	private String topic ;
	private ProducerConfig config ;
	public HighProducerTask(String topic,ProducerConfig config){
		this.topic = topic ;
		this.config = config ;
	}
	@Override
	public void run() {
		int i = 100000000;
		Producer<String, String> producer = new Producer<String, String>(config);
		Calendar calendar = Calendar.getInstance() ;
		calendar.setTime( new Date() );
		while (i<100000100) {
			calendar.add(Calendar.MINUTE, -1);
			String time = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(calendar.getTime()) ;
			String msg = i + ":"+time+":"+i;
			System.out.println(msg);
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
			producer.send(data);
			i++;
		}
		producer.close();
	}

}
