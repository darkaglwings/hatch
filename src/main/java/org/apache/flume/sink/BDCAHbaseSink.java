package org.apache.flume.sink;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bill99.util.JsonUtil;
import com.bill99.util.StringUtil;

public class BDCAHbaseSink extends AbstractSink implements Configurable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(BDCAHbaseSink.class);
	
	//private String zookeeper = "CDH-HUAXIA-00005:2181,CDH-HUAXIA-00006:2181,CDH-HUAXIA-00003:2181";
	private String zookeeper;
	
	private String tableName;
	
	private String rowkey;
	
	private Table table;
	
	private byte[] family;
	
	@Override
	public void configure(Context context) {
		zookeeper = context.getString("zookeeper", "");
		tableName = context.getString("tableName", "");
		rowkey = context.getString("rowkey", "");
		
		if (StringUtil.isEmpty(zookeeper)) {
			throw new RuntimeException("zookeeper can not be null.");
		}
		
		if (StringUtil.isEmpty(tableName)) {
			throw new RuntimeException("tableName can not be null.");
		}
		
		if (StringUtil.isEmpty(rowkey)) {
			throw new RuntimeException("rowkey can not be null.");
		}
		
		LOGGER.info("BDCAHbaseSink zookeeper={}", zookeeper);
		LOGGER.info("BDCAHbaseSink table={}", table);
		LOGGER.info("BDCAHbaseSink rowkey={}", rowkey);
	}

	@Override
	public void start() {
		this.connect();
	}

	@Override
	public void stop () {
		try {
			if (table != null) table.close();
		} catch (IOException e) {
			LOGGER.error("BDCAHbaseSink error.", e);
		}
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;

		// Start transaction
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			Event event = ch.take();
			LOGGER.info("get event={}", event);
			if (event != null) {
				String body = new String(event.getBody(), "UTF-8");
				LOGGER.info("message={}", body);
				if (StringUtil.isNotEmpty(body)) {
					Map<String, Object> map = JsonUtil.convert2Object(body, new HashMap<String, Object>());
					
					StringBuffer sbuf = new StringBuffer(String.valueOf(map.get(rowkey)));
					sbuf.reverse();
					
					Put put = new Put(sbuf.toString().getBytes());
					for (Entry<String, Object> entry : map.entrySet()){
						LOGGER.info("family={}", new String(family, "UTF-8"));
						LOGGER.info("key={}", entry.getKey());
						LOGGER.info("value={}", entry.getValue());
						put.addColumn(family, entry.getKey().getBytes(), String.valueOf(entry.getValue()).getBytes());
					}
					table.put(put);
				}
				txn.commit();
				status = Status.READY;
			} else {
				txn.rollback();
				status = Status.BACKOFF;
			}
		} catch (Exception e) {
			txn.rollback();
			status = Status.BACKOFF;

			LOGGER.error("BDCAHbaseSink error.", e);
		} finally {
			txn.close();
		}
		return status;
	}
	
	protected boolean connect() {
		boolean result = false;
		
		if (table == null) {
			try {
				Configuration conf = HBaseConfiguration.create();
				conf.set("hbase.zookeeper.quorum", zookeeper);
				
				Connection conn = ConnectionFactory.createConnection(conf);
				Admin admin = conn.getAdmin();
				boolean isExists = admin.tableExists(TableName.valueOf(tableName));
				if (isExists) {
					table = conn.getTable(TableName.valueOf(tableName));
					
					HTableDescriptor descriptor = table.getTableDescriptor();
					HColumnDescriptor[] cDescriptor = descriptor.getColumnFamilies();
					family = cDescriptor[0].getName();
				} else {
					LOGGER.error("BDCAHbaseSink error, table does not exists. table={}", tableName);
				}
				
				result = true;
			}catch(Exception e){
				LOGGER.error("BDCAHbaseSink error.", e);
			}
		}
		
		return result;
	}
	
	public static void main(String[] args) {
		try {
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "CDH-HUAXIA-00005:2181,CDH-HUAXIA-00006:2181,CDH-HUAXIA-00003:2181");

			Connection conn = ConnectionFactory.createConnection(conf);
			Admin admin = conn.getAdmin();
			boolean isExists = admin.tableExists(TableName.valueOf("test"));
			if (isExists) {
				Table table = conn.getTable(TableName.valueOf("test"));

				HTableDescriptor descriptor = table.getTableDescriptor();
				HColumnDescriptor[] cDescriptor = descriptor.getColumnFamilies();
				byte[] family = cDescriptor[0].getName();
			} else {
				System.out.println("BDCAHbaseSink error, table does not exists.");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
