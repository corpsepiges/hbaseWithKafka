package com.xz.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.config.HbaseConfig;

public class MyCoprocessor extends BaseRegionObserver {
	private HTableInterface table;
	private List<Put> list;

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		synchronized (list) {
			NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();
			Iterator<byte[]> it = map.keySet().iterator();
			while (it.hasNext()) {
				byte[] key = it.next();
				List<Cell> values = map.get(key);
				for (int i = 0; i < values.size(); i++) {
					Cell cell = values.get(i);
					byte[] rowkey = CellUtil.cloneRow(cell);
					byte[] family = CellUtil.cloneFamily(cell);
					byte[] qualifier = CellUtil.cloneQualifier(cell);
					byte[] value = CellUtil.cloneValue(cell);

					byte[] newRowkey = qualifier;
					byte[] newFamily = family;
					byte[] newQualifier = rowkey;
					byte[] newValue = value;
					
					System.out.println("newRowkey"+":"+Bytes.toString(newRowkey));
					System.out.println("newFamily"+":"+Bytes.toString(newFamily));
					System.out.println("newQualifier"+":"+Bytes.toString(newQualifier));
					System.out.println("newValue"+":"+Bytes.toString(newValue));
					
					Put newPut = new Put(newRowkey);
					newPut.add(newFamily, newQualifier, newValue);
					list.add(put);
				}
			}

			if (list.size() > 1000) {
				table.put(list);
				table.flushCommits();
				list.clear();
				System.out.println("submit");

			}
		}
		super.postPut(e, put, edit, durability);
	}

	@Override
	public void start(CoprocessorEnvironment environment) throws IOException {
		System.out.println("---------------MyCoprocessor-----start-------------");
		table = environment.getTable(TableName.valueOf(HbaseConfig.BUSTABLEINDEX));
		table.setAutoFlushTo(false);
		table.setWriteBufferSize(24 * 1000 * 1000);

		list = new ArrayList<>();

		 Timer timer = new Timer() ;
		 TimerTask task = new MyTimeTask(table, list) ;
		 timer.schedule(task, 1000*60*2, 1000*60*2);

		super.start(environment);
	}

	@Override
	public void stop(CoprocessorEnvironment environment) throws IOException {
		System.out.println("---------------MyCoprocessor-----end-------------");
		if (table != null) {
			table.close();
			table = null;
		}
		super.stop(environment);
	}

}
