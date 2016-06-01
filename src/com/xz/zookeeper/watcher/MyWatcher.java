package com.xz.zookeeper.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class MyWatcher implements Watcher{

	@Override
	public void process(WatchedEvent event) {
		System.out.println("mywatcher"+event.getPath());
	}

}
