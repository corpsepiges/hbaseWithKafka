package com.xz.message;

import java.io.IOException;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;

public class Msg {
	private static String uid = "xzparty@sina.com" ;
	private static String key = "cf3db2ed7220d81e4fda" ;
	public static boolean sendMessage(String phone,String text){
		boolean bo = false ;
		try {
			HttpClient client = new HttpClient();
			PostMethod post = new PostMethod("http://gbk.sms.webchinese.cn");
			post.addRequestHeader("Content-Type", "application/x-www-form-urlencoded;charset=gbk");// 在头文件中设置转码
			NameValuePair[] data = { new NameValuePair("Uid", uid), new NameValuePair("Key", key),
					new NameValuePair("smsMob", phone), new NameValuePair("smsText", text) };
			post.setRequestBody(data);

			client.executeMethod(post);
			int statusCode = post.getStatusCode();
			if (statusCode==200) {
				String result = new String(post.getResponseBodyAsString().getBytes("gbk"));
				int type = Integer.parseInt(result) ;
				if (type>0) {
					bo = true ;
				}
			}
			post.releaseConnection();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (HttpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bo ;
	}
	
	public static void main(String[] args) throws Exception {
		HttpClient client = new HttpClient();
		PostMethod post = new PostMethod("http://gbk.sms.webchinese.cn");
		post.addRequestHeader("Content-Type", "application/x-www-form-urlencoded;charset=gbk");// 在头文件中设置转码
		NameValuePair[] data = { new NameValuePair("Uid", "xzparty@sina.com"), new NameValuePair("Key", "cf3db2ed7220d81e4fda"),
				new NameValuePair("smsMob", "15262904389"), new NameValuePair("smsText", "连云港 今天白天：小雨 最高温度：23℃ 今天夜间：小雨 最低温度：16℃  ") };
		post.setRequestBody(data);

		client.executeMethod(post);
		Header[] headers = post.getResponseHeaders();
		int statusCode = post.getStatusCode();
		System.out.println("statusCode:" + statusCode);
		for (Header h : headers) {
			System.out.println(h.toString());
		}
		String result = new String(post.getResponseBodyAsString().getBytes("gbk"));
		System.out.println(result);
		post.releaseConnection();
	}
}
