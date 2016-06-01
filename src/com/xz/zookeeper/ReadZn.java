package com.xz.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.xz.config.Config;
import com.xz.zookeeper.watcher.MyWatcher;

public class ReadZn {
	public static void main(String[] args) {
		Watcher watcher = new MyWatcher() ;
		try {
			ZooKeeper zooKeeper = new ZooKeeper(Config.getMap().get("zookeeperList"), 2000, watcher) ;
			List<String> list = zooKeeper.getChildren("/", true) ;
			ReadZn.getAll(list, zooKeeper,"/");
			zooKeeper.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void getAll(List<String> list,ZooKeeper zooKeeper,String parent) throws KeeperException, InterruptedException{
		for (int i = 0; i < list.size(); i++) {
			String path = parent+list.get(i) ;
			Stat stat = zooKeeper.exists(path, false) ;
			byte[] value = zooKeeper.getData(path, false, stat) ;
			System.out.println(path+"--------"+Bytes.toString(value));
			if (stat!=null) {
				List<String> list2 = zooKeeper.getChildren(path, false) ;
				ReadZn.getAll(list2, zooKeeper,path+"/");
			}
		}
	}
}
