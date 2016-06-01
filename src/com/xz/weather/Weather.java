package com.xz.weather;

import java.util.Map;

public class Weather {
	/** 城市 */
	private String city;
	/** 白天天气状态 */
	private String dayStatus;
	/** 夜间天气状态 */
	private String nightStatus;
	/** 白天温度 */
	private String dayTemperature;
	/** 夜间温度 */
	private String nightTemperature;
	/** 白天风向 */
	private String dayDirection;
	/** 夜间风向 */
	private String nightDirection;
	/** 白天风力 */
	private String dayPower;
	/** 夜间风力 */
	private String nightPower;
	/** 哪天 */
	private int day ;

	public Weather(Map<String, String> map) {
		this.city = map.get("city") ;
		this.dayStatus = map.get("status1") ;
		this.nightStatus = map.get("status2") ;
		this.dayTemperature = map.get("temperature1") ;
		this.nightTemperature = map.get("temperature2") ;
		this.dayDirection = map.get("direction1") ;
		this.nightDirection = map.get("direction2") ;
		this.dayPower = map.get("power1") ;
		this.nightPower = map.get("power2") ;
		this.day = Integer.parseInt(map.get("day")) ;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer() ;
		String dayString = "" ;
		switch (this.day) {
		case 0:
			dayString = "今天" ;
			break;
		case 1:
			dayString = "明天" ;
			break;
		case 2:
			dayString = "后天" ;
			break;
		default:
			break;
		}
		sb.append(this.city).append(" ").append("\t") ;
		sb.append(dayString).append("白天：").append(this.dayStatus).append(",最高温度：").append(dayTemperature).append("° ,").append(this.dayDirection).append(this.dayPower).append("级。").append("\t") ;
		sb.append(dayString).append("夜间：").append(this.nightStatus).append(",最高温度：").append(nightTemperature).append("° ,").append(this.nightDirection).append(this.nightPower).append("级。").append("\t") ;
		sb.append("\n") ;
		return sb.toString();
	}
	
}
