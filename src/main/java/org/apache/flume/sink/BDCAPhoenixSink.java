package org.apache.flume.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bill99.util.JsonUtil;
import com.bill99.util.StringUtil;

public class BDCAPhoenixSink  extends AbstractSink implements Configurable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(BDCAPhoenixSink.class);
	
	private String url;
	
	private String className;
	
	private String username;
	
	private String password;
	
	private String sql;
	
	private String rowkey;
	
	private String paramNames;
	
	private Connection conn;
	
	private PreparedStatement pstmt;
	
	@Override
	public void configure(Context context) {
		url = context.getString("url", "");
		className = context.getString("className", "");
		username = context.getString("username", "");
		password = context.getString("password", "");
		sql = context.getString("sql", "");
		rowkey = context.getString("rowkey", "");
		paramNames = context.getString("paramNames", "");
		
		if (StringUtil.isEmpty(url)) {
			throw new RuntimeException("url can not be null.");
		}
		
		if (StringUtil.isEmpty(className)) {
			throw new RuntimeException("className can not be null.");
		}
		
		if (StringUtil.isEmpty(sql)) {
			throw new RuntimeException("sql can not be null.");
		}
		
		if (StringUtil.isEmpty(rowkey)) {
			throw new RuntimeException("rowkey can not be null.");
		}
		
		if (sql.toLowerCase().indexOf("select") != -1 || sql.toLowerCase().indexOf("delete") != -1 || sql.toLowerCase().indexOf("truncate") != -1) {
			throw new RuntimeException("illeage sql, only insert, update or upsert supported.");
		}
		
		LOGGER.info("BDCAPhoenixSink url={}", url);
		LOGGER.info("BDCAPhoenixSink className={}", className);
		LOGGER.info("BDCAPhoenixSink username={}", username);
		LOGGER.info("BDCAPhoenixSink password={}", password);
		LOGGER.info("BDCAPhoenixSink sql={}", sql);
		LOGGER.info("BDCAPhoenixSink rowkey={}", rowkey);
		LOGGER.info("BDCAPhoenixSink paramNames={}", paramNames);
	}
	
	@Override
	public void start() {
		this.connect();
	}

	@Override
	public void stop () {
		try {
			if (pstmt != null) pstmt.close();
		} catch (SQLException e) {
			pstmt = null;
			LOGGER.error("can not close preparedstatment.", e);
		}
		
		try {
			if (conn != null) conn.close();
		} catch (SQLException e) {
			conn = null;
			LOGGER.error("can not close connection.", e);
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
					pstmt = conn.prepareStatement(sql);
					if (StringUtil.isNotEmpty(paramNames)) {
						String[] params = paramNames.split(",");
						for (int i = 0; i < params.length; i++) {
							if (params[i].equals(rowkey)) {
								StringBuffer sbuf = new StringBuffer(String.valueOf(map.get(params[i])));
								sbuf.reverse();
								
								pstmt.setString(i + 1, sbuf.toString());
							} else {
								pstmt.setString(i + 1, String.valueOf(map.get(params[i])));
							}
						}
					}
					pstmt.executeUpdate();
				}
				txn.commit();
				status = Status.READY;
			} else {
				LOGGER.info("event is null.");
				txn.rollback();
				status = Status.BACKOFF;
			}
		} catch (Exception e) {
			txn.rollback();
			status = Status.BACKOFF;

			LOGGER.error("BDCAPhoenixSink error.", e);
		} finally {
			txn.close();
		}
		return status;
	}
	
	private boolean connect() {
		boolean result = false;
		
		try {
			if (conn == null || conn.isClosed()) {
				conn = null;
				Class.forName(className).newInstance();
				conn = DriverManager.getConnection(url, username, password);
			}
			
			result = true;
		} catch (InstantiationException e) {
			LOGGER.error("custom flume db source error.", e);
		} catch (IllegalAccessException e) {
			LOGGER.error("custom flume db source error.", e);
		} catch (ClassNotFoundException e) {
			LOGGER.error("custom flume db source error.", e);
		} catch (SQLException e) {
			LOGGER.error("custom flume db source error.", e);
		} catch (Exception e) {
			LOGGER.error("custom flume db source error.", e);
		}
		
		LOGGER.info("BDCAPhoenixSink connected = {}", result);
		
		return result;
	}
	
	public static void main(String[] args) {
		String url = "jdbc:phoenix:CDH-HUAXIA-00005,CDH-HUAXIA-00006,CDH-HUAXIA-00003:2181";
		String className = "org.apache.phoenix.jdbc.PhoenixDriver";
		
		try {
			String sql = "select * from \"common\".\"tf_evt_cps_stl_order\"";
			
			Class.forName(className).newInstance();
			Connection conn = DriverManager.getConnection(url, "", "");
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
