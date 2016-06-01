package com.xz.init;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.config.HbaseConfig;
import com.xz.config.HbaseConnection;

public class TableSplit {
	public static void main(String[] args) {
		try {
			HConnection connection = HbaseConnection.getHbaseConnection() ;
			HBaseAdmin admin = new HBaseAdmin(connection) ;
			admin.split(HbaseConfig.BUSTABLE,Bytes.toBytes("100000099"));
			admin.close(); 
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
