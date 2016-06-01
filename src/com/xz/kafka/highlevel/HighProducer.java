package com.xz.kafka.highlevel;

import java.util.Properties;

import kafka.producer.ProducerConfig;

import com.xz.config.KafkaConfig;

public class HighProducer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", KafkaConfig.BROKERLIST);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//key 的序列化方式
		//kafka.serializer.NullEncoder
		//kafka.serializer.DefaultEncoder
		//kafka.serializer.StringEncoder
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		//自定义partition 集群方式
		//props.put("partitioner.class", "com.xz.kafka.partitioner.MyPartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);

		String topic = KafkaConfig.TOPIC;

		// 创建producer
//		Producer<Strinsg, String> producer = new Producer<String, String>(config);
		/*
		int i = 1;
		Calendar calendar = Calendar.getInstance() ;
		calendar.setTime( new Date() );
		while (true) {
			calendar.add(Calendar.MINUTE, -1);
			String time = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(calendar.getTime()) ;
			String msg = i + ":"+time+":什么意思";
			System.out.println(msg);
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
			producer.send(data);
			i++;
		}

//		producer.close();
*/
		for (int i = 0; i < 1; i++) {
			HighProducerTask kafkaProducerTask = new HighProducerTask(topic, config) ;
			Thread thread1 = new Thread(kafkaProducerTask) ;
			thread1.start();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
