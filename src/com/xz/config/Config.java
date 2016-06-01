package com.xz.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


public class Config {
	private static Map<String, String> map = new HashMap<>() ;
	
	static{
		try {
			Properties properties = new Properties();
			InputStream is = Config.class.getClassLoader().getResourceAsStream("config.properties");
			properties.load(is);
			@SuppressWarnings("unused")
			Iterator it = properties.keySet().iterator() ;
			while (it.hasNext()) {
				String key = (String)it.next() ;
				String value = properties.getProperty(key) ;
				map.put(key, value) ;
			}
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Map<String, String> getMap() {
		return map;
	}
	
}


