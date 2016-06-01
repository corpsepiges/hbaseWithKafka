package com.xz.kafka.highlevel;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

public class HighComsumerTimer extends TimerTask{
	private Map<HTableInterface, List<Put>> map ;
	
	public HighComsumerTimer(Map<HTableInterface, List<Put>> map){
		this.map = map ;
	}

	@Override
	public void run() {
		System.out.println("clear comsumer cache ");
		Iterator<HTableInterface> it = map.keySet().iterator() ;
		while (it.hasNext()) {
			HTableInterface table = it.next() ;
			List<Put> list = map.get(table) ;
			synchronized (list) {
				if (list.size()>0) {
					try {
						System.out.println("flash");
						table.put(list);
						table.flushCommits();
						list.clear();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
			}
			
		}
		
	}

}
