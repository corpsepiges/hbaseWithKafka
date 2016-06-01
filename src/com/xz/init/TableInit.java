package com.xz.init;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.config.HbaseConfig;
import com.xz.config.HbaseConnection;

public class TableInit {
	public static void main(String[] args) {
		 createTable();
//		getDate();

	}

	private static void getDate() {
		try {
			HConnection connection = HbaseConnection.getHbaseConnection();
			HTableInterface hTableInterface = connection.getTable(HbaseConfig.BUSTABLE);

			byte[] rowkey = Bytes.toBytes("904");
			byte[] family = HbaseConfig.BUSTABLE_FAMILY;
			byte[] qualifier = Bytes.toBytes("2016-04-21 00:16:52");
			Get get = new Get(rowkey);
			get.addColumn(family, qualifier) ;
//			get.addFamily(family);

			Result result = hTableInterface.get(get);
			System.out.println(result);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void createTable() {
		try {
			HBaseAdmin admin = new HBaseAdmin(HbaseConnection.getHbaseConnection());
			if (!admin.tableExists(HbaseConfig.BUSTABLE)) {

				HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(HbaseConfig.BUSTABLE));

				HColumnDescriptor family = new HColumnDescriptor(HbaseConfig.BUSTABLE_FAMILY);

				descriptor.addFamily(family);
				admin.createTable(descriptor);
			}

			if (!admin.tableExists(HbaseConfig.BUSTABLEINDEX)) {

				HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(HbaseConfig.BUSTABLEINDEX));

				HColumnDescriptor family = new HColumnDescriptor(HbaseConfig.BUSTABLEINDEX_FAMILY);

				descriptor.addFamily(family);
				admin.createTable(descriptor);
			}

		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
