package com.xz.kafka.command;

import com.xz.config.KafkaConfig;

import kafka.admin.AddPartitionsCommand;
import kafka.admin.CreateTopicCommand;
import kafka.admin.DeleteTopicCommand;

public class Command {
	public static void main(String[] args) {
		String topic = "splits" ;
		deletetopic(topic) ;
		createtopic(topic);
		addPartition(topic);
	}
	private static void createtopic(String topic) {
		String [] options = new String[]{
				"--zookeeper",
				KafkaConfig.ZOOKEEPERLIST,
				"--topic",
				topic,
				"--partition",
				"2"
		};
		CreateTopicCommand.main(options);
	}
	private static void deletetopic(String topic) {
		String [] options = new String[]{
				"--zookeeper",
				KafkaConfig.ZOOKEEPERLIST,
				"--topic",
				topic
		};
		DeleteTopicCommand.main(options);
	}
	private static void addPartition(String topic) {
		String [] options = new String[]{
				"--zookeeper",
				KafkaConfig.ZOOKEEPERLIST,
				"--topic",
				topic,
				"--partition",
				//加几个分区
				"1"
		};
		AddPartitionsCommand.main(options);
	}
}
