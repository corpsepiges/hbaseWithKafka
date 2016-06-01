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
import com.xz.hbase.endpoint.protocol.StatisticsProto.CountRequest;
import com.xz.hbase.endpoint.protocol.StatisticsProto.CountResponse;
import com.xz.hbase.endpoint.protocol.StatisticsProto.RowKeyCountService;

public class RowCountClient {
	public static void main(String[] args) {
		try {
			byte[] tableName = HbaseConfig.BUSTABLE ;
			HConnection connection = HbaseConnection.getHbaseConnection() ;
			HTableInterface table = connection.getTable(tableName) ;
			String startKey = "100000098" ;
			String endKey = "100000100" ;
			
			final CountRequest countRequest = CountRequest.newBuilder().setStartKey(startKey).setEndKey(endKey).build() ;
			Call<RowKeyCountService, Long> call = new Call<RowKeyCountService, Long>() {
				
				@Override
				public Long call(RowKeyCountService service) throws IOException {
					ServerRpcController controller = new ServerRpcController() ;
					BlockingRpcCallback<CountResponse> rpcCallback = new BlockingRpcCallback<CountResponse>() ;
					service.getRowCount(controller, countRequest, rpcCallback);
					CountResponse countResponse = rpcCallback.get() ;
					if (controller.failedOnException()) {
						throw controller.getFailedOn() ;
					}
					long count = countResponse.getCount() ;
					return count;
				}
			};
			Map<byte[], Long> map = table.coprocessorService(RowKeyCountService.class,Bytes.toBytes(startKey),Bytes.toBytes(endKey),call) ;
			Iterator<Long> it = map.values().iterator() ;
			while (it.hasNext()) {
				System.out.println(it.next());
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
