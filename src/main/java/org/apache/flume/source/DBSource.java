package org.apache.flume.source;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bill99.util.JsonUtil;
import com.bill99.util.StringUtil;


public class DBSource extends AbstractSource implements Configurable, PollableSource {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DBSource.class);
	
	private String url;
	
	private String className;
	
	private String username;
	
	private String password;
	
	private String sql;
	
	private String startTime;
	
	private String interval;
	
	private Connection conn;
	
	private PreparedStatement pstmt;
	
	private ResultSetMetaData rsmd;
	
	private ResultSet rs;

	@Override
	public void configure(Context context) {
		url = context.getString("url", "");
		className = context.getString("className", "");
		username = context.getString("username", "");
		password = context.getString("password", "");
		sql = context.getString("sql", "");
		startTime = context.getString("startTime", "");
		interval = context.getString("interval", "");
		
		if (StringUtil.isEmpty(url)) {
			throw new RuntimeException("url can not be null.");
		}
		
		if (StringUtil.isEmpty(className)) {
			throw new RuntimeException("className can not be null.");
		}
		
		if (StringUtil.isEmpty(sql)) {
			throw new RuntimeException("sql can not be null.");
		}
		
		if (sql.toLowerCase().indexOf("insert") != -1 || sql.toLowerCase().indexOf("delete") != -1 || sql.toLowerCase().indexOf("truncate") != -1) {
			throw new RuntimeException("illeage sql, only select supported.");
		}
		
		if (startTime == null || startTime.length() < 1) {
			startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
		}
		
		if (interval == null || interval.length() < 1) {
			interval = "60000";
		}
	}

	@Override
	public void start() {
		this.connect();
	}

	@Override
	public void stop () {
		try {
			if (rs != null) rs.close();
		} catch (SQLException e) {
			rs = null;
			LOGGER.error("can not close resultset.", e);
		}
		
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

		try {
			String current = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

			pstmt = conn.prepareStatement(sql);
			pstmt.setObject(1, startTime);
			pstmt.setObject(2, current);

			Map<String, Object> map;
			List<Event> list = new ArrayList<Event>();
			rs = pstmt.executeQuery();
			rsmd = rs.getMetaData();
			while (rs.next()) {
				map = new HashMap<String, Object>();
				for(int i = 1; i <= rsmd.getColumnCount(); i++) {
					map.put(rsmd.getColumnName(i), rs.getObject(i));
				}

				list.add(this.eventBuilder(map));
			}

			getChannelProcessor().processEventBatch(list);

			status = Status.READY;
			this.saveLastExecuteTime(current);

			long sleep;
			try {
				sleep = Long.parseLong(interval);
			} catch (NumberFormatException e) {
				sleep = 60 * 1000;
			}

			Thread.sleep(sleep);
		} catch (Exception e) {
			status = Status.BACKOFF;
			LOGGER.error("custom flume db souce error.", e);
		} finally {
			try {
				if (rs != null) rs.close();
			} catch (SQLException e) {
				rs = null;
				LOGGER.error("can not close resultset.", e);
			}
			
			try {
				if (pstmt != null) pstmt.close();
			} catch (SQLException e) {
				pstmt = null;
				LOGGER.error("can not close preparedstatment.", e);
			}
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
		}
		
		return result;
	}
	
	private Event eventBuilder(Map<String, Object> map) {
		Event result = null;
		if (map != null && map.size() > 0) {
			result = EventBuilder.withBody(JsonUtil.convert2Json(map), Charset.forName("UTF-8"));
		}
		
		LOGGER.info("data={}", result);
		
		return result;
	}
	
	private boolean saveLastExecuteTime(String current) {
		boolean result = false;
		
		//TODO: to be changed
		this.startTime = current;
		result = true;
		
		return result;
	}
}
