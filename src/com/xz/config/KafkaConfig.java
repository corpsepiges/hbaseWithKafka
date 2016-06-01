package com.xz.config;

public class KafkaConfig {
	public static String ZOOKEEPERLIST = Config.getMap().get("zookeeperList") ;
	public static String TOPIC = Config.getMap().get("topic") ;
	public static String GROUPID = Config.getMap().get("groupid") ;
	
	public static String BROKERLIST = Config.getMap().get("metadatabrokerlist") ;
	
}
