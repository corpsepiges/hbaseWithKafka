package com.xz.kafka.highlevel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import com.xz.config.HbaseConfig;
import com.xz.config.HbaseConnection;
import com.xz.config.KafkaConfig;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class HighComsumer {
	private ConsumerConnector consumer = null ;
	private String topic = KafkaConfig.TOPIC ;
	private ExecutorService executorService = null ;
	
	private static Map<HTableInterface, List<Put>> dataMap = new HashMap<>() ;
	
	private ConsumerConfig createConsumerConfig(){
		Properties prop = new Properties() ;
		prop.put("zookeeper.connect", KafkaConfig.ZOOKEEPERLIST) ;
		prop.put("auto.commit.enable", "true") ;
		prop.put("auto.commit.interval.ms", "60000") ;
		prop.put("group.id", KafkaConfig.GROUPID) ;
		return new ConsumerConfig(prop) ;
	}
	
	public HighComsumer(){
		ConsumerConfig config = createConsumerConfig() ;
		consumer = Consumer.createJavaConsumerConnector(config) ;
	}
	
	public void run(int num){
		try {
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>() ;
			topicCountMap.put(topic, new Integer(num)) ;
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap) ;
			List<KafkaStream<byte[], byte[]>> steams = consumerMap.get(topic) ;
			
			executorService = Executors.newFixedThreadPool(num) ;
					int threadNumber = 0 ;
			HConnection hConnection = HbaseConnection.getHbaseConnection() ;
			for (int i = 0; i < steams.size(); i++) {
				HTableInterface hTableInterface = hConnection.getTable(HbaseConfig.BUSTABLE) ;
				hTableInterface.setAutoFlushTo(false);
				hTableInterface.setWriteBufferSize(24 * 1024 * 1024l);
				
				List<Put> list = new ArrayList<>() ;
				dataMap.put(hTableInterface, list) ;
				
				executorService.submit(new HighComsumerTask(steams.get(i), threadNumber,hTableInterface,list));
				threadNumber++;
			}
			Timer timer = new Timer() ;
			HighComsumerTimer highComsumerTimer = new HighComsumerTimer(dataMap) ;
			timer.schedule(highComsumerTimer, 1000*60*2, 1000*60*2) ;
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void shutdown(){
		if (consumer!=null) {
			consumer.shutdown();
		}
		if (executorService!=null) {
			executorService.shutdown();
		}
	}
	public static void main(String[] args) {
		HighComsumer consumer = new HighComsumer() ;
		consumer.run(5);
//		consumer.shutdown();
	}
}
