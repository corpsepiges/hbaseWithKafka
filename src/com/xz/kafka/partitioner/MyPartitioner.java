package com.xz.kafka.partitioner;

import kafka.producer.Partitioner;

public class MyPartitioner implements Partitioner<String>{
	
	@Override
	public int partition(String key, int num) {
		return Math.abs(key.hashCode()) % num ;
	}

}
