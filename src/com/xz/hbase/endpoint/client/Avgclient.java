package com.xz.hbase.endpoint.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;
import com.xz.config.HbaseConfig;
import com.xz.config.HbaseConnection;
import com.xz.hbase.endpoint.protocol.StatisticsProto.AvgRequest;
import com.xz.hbase.endpoint.protocol.StatisticsProto.AvgResponse;
import com.xz.hbase.endpoint.protocol.StatisticsProto.AvgService;

public class Avgclient {
	public static void main(String[] args) {
		try {
			byte[] tableName = HbaseConfig.BUSTABLE ;
			HConnection connection = HbaseConnection.getHbaseConnection() ;
			HTableInterface table = connection.getTable(tableName) ;
			String startKey = "100000098" ;
			String endKey = "100000100" ;
			
			final AvgRequest avgRequest = AvgRequest.newBuilder().setStartRowKey(startKey).setEndRowKey(endKey).build() ;
			Call<AvgService, String[]> call = new Call<AvgService, String[]>() {
				
				@Override
				public String[] call(AvgService service) throws IOException {
					ServerRpcController controller = new ServerRpcController() ;
					BlockingRpcCallback<AvgResponse> rpcCallBack = new BlockingRpcCallback<AvgResponse>() ;
					service.getResponse(controller, avgRequest, rpcCallBack);
					AvgResponse avgResponse = rpcCallBack.get() ;
					if (controller.failedOnException()) {
						throw controller.getFailedOn() ;
					}
					String num = avgResponse.getNum() ;
					String sum = avgResponse.getSum() ;
					String[] strings = new String[2] ;
					strings[0] = num ;
					strings[1] = sum ;
					return strings;
				}
			};
			
			Map<byte[], String[]> map = table.coprocessorService(AvgService.class,Bytes.toBytes(startKey),Bytes.toBytes(endKey),call) ;
			Iterator<String[]> it = map.values().iterator() ;
			while (it.hasNext()) {
				String[] strings = it.next() ;
				String num = strings[0] ;
				String sum = strings[1] ;
				System.out.println("数量："+num+"，总和："+sum);
			}
		
		
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ServiceException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		
	}
}
