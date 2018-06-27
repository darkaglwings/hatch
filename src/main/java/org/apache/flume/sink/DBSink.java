package org.apache.flume.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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

public class DBSink extends AbstractSink implements Configurable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DBSink.class);
	
	private String url;
	
	private String className;
	
	private String username;
	
	private String password;
	
	private String sql;
	
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
		
		if (sql.toLowerCase().indexOf("select") != -1 || sql.toLowerCase().indexOf("delete") != -1 || sql.toLowerCase().indexOf("truncate") != -1) {
			throw new RuntimeException("illeage sql, only insert, update or upsert supported.");
		}
		
		LOGGER.info("DBSink url={}", url);
		LOGGER.info("DBSink className={}", className);
		LOGGER.info("DBSink username={}", username);
		LOGGER.info("DBSink password={}", password);
		LOGGER.info("DBSink sql={}", sql);
		LOGGER.info("DBSink paramNames={}", paramNames);
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
							pstmt.setObject(i + 1, map.get(params[i]));
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

			LOGGER.error("DBSink error.", e);
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
		
		LOGGER.info("DBSink connected = {}", result);
		
		return result;
	}
}
