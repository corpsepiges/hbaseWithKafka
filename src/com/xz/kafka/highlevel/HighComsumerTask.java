package com.xz.kafka.highlevel;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.xz.config.HbaseConfig;
import com.xz.hbase.Contents;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class HighComsumerTask implements Runnable {
	private Logger logger = Logger.getLogger(HighComsumerTask.class);
	private KafkaStream<byte[], byte[]> m_stream;
	private int num;

	private List<Put> list;
	private HTableInterface hTableInterface;
	

	public HighComsumerTask(KafkaStream<byte[], byte[]> m_stream, int num, HTableInterface hTableInterface,
			List<Put> list) {
		this.m_stream = m_stream;
		this.num = num;
		this.hTableInterface = hTableInterface;
		this.list = list;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> iterator = m_stream.iterator();
		System.out.println("m_threadNumber:" + num);
		while (iterator.hasNext()) {
			String msg = new String(iterator.next().message());
			System.out.println(msg);
			this.insert2Test(hTableInterface, list, msg);
		}
	}

	private void insert2Test(HTableInterface hTableInterface, List<Put> list, String msg) {
		try {
			String[] strings = msg.split(":") ;
			byte[] rowkey = Bytes.toBytes(strings[0]) ;
			byte[] family = HbaseConfig.BUSTABLE_FAMILY ;
			byte[] qualifier = Bytes.toBytes(strings[1]) ;
			byte[] value = Bytes.toBytes(strings[2]) ;
			
			Put put = new Put(rowkey) ;
			put.add(family, qualifier, value) ;
			list.add(put) ;
			if (list.size()>Contents.BUFFERSIZE) {
				hTableInterface.put(list);
				hTableInterface.flushCommits();
				list.clear();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
