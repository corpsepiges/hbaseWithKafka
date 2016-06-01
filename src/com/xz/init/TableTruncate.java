package com.xz.init;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;

import com.xz.config.HbaseConfig;
import com.xz.config.HbaseConnection;

public class TableTruncate {
	public static void main(String[] args) {
		byte[] tableName = HbaseConfig.BUSTABLE ;
		truncateTable(tableName);
	}

	private static void truncateTable(byte[] tableName) {
		HConnection connection = null ;
		HBaseAdmin admin = null ;
		try {
			connection = HbaseConnection.getHbaseConnection() ;
			admin = new HBaseAdmin(connection) ;
			if (!admin.tableExists(tableName)) {
				return ;
			}
			admin.disableTable(tableName);
			admin.truncateTable(TableName.valueOf(tableName), true);
			if (!admin.isTableEnabled(tableName)) {
				admin.enableTable(tableName);
			}
			
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				admin.close();
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
