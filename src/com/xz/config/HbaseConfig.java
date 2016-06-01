package com.xz.config;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseConfig {
	
	
	public static byte[] BUSTABLE =  Bytes.toBytes("business") ;
	public static byte[] BUSTABLEINDEX =  Bytes.toBytes("businessindex") ;
	
	public static byte[] BUSTABLE_FAMILY =  Bytes.toBytes("cf") ;
	public static byte[] BUSTABLEINDEX_FAMILY =  Bytes.toBytes("cf") ;
	
	
}
