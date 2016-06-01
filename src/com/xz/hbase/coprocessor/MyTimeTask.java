package com.xz.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.TimerTask;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

public class MyTimeTask extends TimerTask{
	
	private HTableInterface tableInterface ;
	private List<Put> list ;
	
	public MyTimeTask(HTableInterface tableInterface,List<Put> list){
		this.tableInterface = tableInterface ;
		this.list = list ;
	}
	@Override
	public void run() {
		System.out.println("clear coprocessor cache ");
		if (list.size()>0) {
			try {
				synchronized (list) {
					tableInterface.put(list);
					tableInterface.flushCommits();
					list.clear();
					System.out.println("flash");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

}
