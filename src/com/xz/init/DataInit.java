package com.xz.init;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.config.HbaseConfig;
import com.xz.config.HbaseConnection;

public class DataInit {
	public static void main(String[] args) {
		try {
			byte[] tableName = HbaseConfig.BUSTABLE ;
			byte[] family = HbaseConfig.BUSTABLE_FAMILY ;
			HConnection hConnection = HbaseConnection.getHbaseConnection() ;
			HTableInterface hTableInterface = hConnection.getTable(tableName) ;
			List<Put> list = new ArrayList<Put>() ;
			
			Put put = new Put(Bytes.toBytes("100000099")) ;
			put.add(family, Bytes.toBytes("2000-00-00-12-12-12"), Bytes.toBytes("13")) ;
			put.add(family, Bytes.toBytes("2000-00-00-12-12-13"), Bytes.toBytes("14")) ;
			list.add(put) ;
			
			Put put2 = new Put(Bytes.toBytes("100000098")) ;
			put2.add(family, Bytes.toBytes("2000-00-00-12-12-12"), Bytes.toBytes("1")) ;
			list.add(put2) ;
			
			hTableInterface.put(list) ;
			hTableInterface.close() ;
			hConnection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
