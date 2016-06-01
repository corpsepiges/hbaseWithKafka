package com.xz.config;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class HbaseConnection {
	private static Configuration hConfiguration = null;
	static{
		hConfiguration = HBaseConfiguration.create();
	}
	public static HConnection getHbaseConnection(){
		HConnection connection = null ;
		try {
			connection = HConnectionManager.createConnection(hConfiguration);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return connection ;
	}
	
	public static void releaseConnection(HConnection hConnection){
		if (hConnection!=null && !hConnection.isClosed()) {
			try {
				hConnection.close();
				hConnection = null ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
