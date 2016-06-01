package com.xz.kafka.command;

import com.xz.config.KafkaConfig;

import kafka.admin.AddPartitionsCommand;
import kafka.admin.CreateTopicCommand;
import kafka.admin.DeleteTopicCommand;

public class Command {
	public static void main(String[] args) {
		
		deletetopic() ;
		createtopic();
		 addPartition();
	}
	private static void createtopic() {
		String [] options = new String[]{
				"--zookeeper",
				KafkaConfig.ZOOKEEPERLIST,
				"--topic",
				KafkaConfig.TOPIC,
				"--partition",
				"2",
		};
		CreateTopicCommand.main(options);
	}
	private static void deletetopic() {
		String [] options = new String[]{
				"--zookeeper",
				KafkaConfig.ZOOKEEPERLIST,
				"--topic",
				KafkaConfig.TOPIC
		};
		DeleteTopicCommand.main(options);
	}
	private static void addPartition() {
		String [] options = new String[]{
				"--zookeeper",
				KafkaConfig.ZOOKEEPERLIST,
				"--topic",
				KafkaConfig.TOPIC,
				"--partition",
				//加几个分区
				"2"
		};
		AddPartitionsCommand.main(options);
	}
}
