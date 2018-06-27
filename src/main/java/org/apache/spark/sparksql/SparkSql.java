package org.apache.spark.sparksql;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sparksql.model.Person;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;
import org.frame.repository.sql.jdbc.IJDBC;
import org.frame.repository.sql.jdbc.impl.JDBC;

public class SparkSql implements Serializable {

	private static final long serialVersionUID = 5429503119151447384L;
	
	/*public void thriftServer() {
		String className = "org.apache.hadoop.hive.jdbc.HiveDriver";
		String url = "jdbc:hive2://192.168.15.169:10000";
		//String url = "spark://hdpdev-as00017:7077";
		String username = "root";
		String password = "111111";
		
		IJDBC jdbc = new JDBC(className, url, username, password);
		List<Map<String, Object>> list = (List<Map<String, Object>>) jdbc.select("select * from words");
		String key;
		for (Map<String, Object> map : list) {
			for (Iterator<String> iterator = map.keySet().iterator(); iterator.hasNext();) {
				key = iterator.next();
				System.out.println(key + ": " + map.get(key));
			}
			System.out.println("===========================");
		}
	}*/
	
	public void teenager() {
		String appName = "demo";
		String master = "spark://hdpdev-as00017:7077";
		//String master = "yarn-cluster";
		String url = "hdfs://192.168.15.169:9000/user/root/input/person";
		//String hadoop_home = "/usr/hadoop-2.6.0";

		//SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		/*conf.set("spark.driver.host", "192.168.15.169");
		conf.set("spark.driver.port", "7077");
		System.out.println(conf.get);
		conf.set("spark.yarn.app.id", "1");
		conf.set("spark.yarn.access.namenodes", "hdfs://192.168.15.169:9000");
		conf.setMaster("yarn-cluster");
		System.out.println("111111111111");*/
		Date start = new Date();
		System.out.println("init start...");
		
		JavaSparkContext ctx = new JavaSparkContext(master, appName);
		//ctx.addJar("hdfs://192.168.15.169:9000/user/root/input/spark.jar");
		ctx.addJar("./sparksql_lib/spark.jar");
		SQLContext sqlContext = new SQLContext(ctx);

		Date end = new Date();
		System.out.println("init end...");
		System.out.println(end.getTime() - start.getTime());
		
		System.out.println("load data...");
		start = new Date();
		//JavaRDD<Person> people = ctx.textFile(url).map(
		JavaRDD<Person> people = ctx.textFile(url).map(
				new Function<String, Person>() {

					private static final long serialVersionUID = -247352976681525204L;

					@Override
					public Person call(String line) {
						String[] parts = line.split(" ");

						Person person = new Person();
						person.setId(Long.parseLong(parts[0]));
						person.setName(parts[1]);
						person.setAge(Integer.parseInt(parts[2]));

						return person;
					}
				});

		DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
		schemaPeople.registerTempTable("person");

		end = new Date();
		System.out.println("load data end.");
		System.out.println(end.getTime() - start.getTime());
		
		System.out.println("select data...");
		start = new Date();
		DataFrame teenagers = sqlContext.sql("SELECT id, name, age FROM person WHERE age >= 13 AND age <= 19");
		List<Map<String, String>> teenagerNames = teenagers.toJavaRDD().map(new Function<Row, Map<String, String>>() {
			
			private static final long serialVersionUID = 5617120094452419298L;

			@Override
			public Map<String, String> call(Row row) {
				Map<String, String> map = new HashMap<String, String>();
				map.put("id", String.valueOf(row.getLong(0)));
				map.put("name", row.getString(1));
				map.put("age", String.valueOf(row.getInt(2)));
				
				return map;
			}
		}).collect();

		end = new Date();
		System.out.println("select data end.");
		System.out.println(end.getTime() - start.getTime());
		
		System.out.println("===========================");
		String key;
		for (Map<String, String> map : teenagerNames) {
			for (Iterator<String> iterator = map.keySet().iterator(); iterator.hasNext();) {
				key = iterator.next();
				System.out.println(key + ": " + map.get(key));
			}
			System.out.println("===========================");
		}
		
		/*for (String name: teenagerNames) {
			System.out.println(name);
		}*/
		
		//teenagers = sqlContext.sql("insert into table person(id, name, age) values(4, 'aa', 12)");
		teenagers.intersect(schemaPeople);
		schemaPeople.save(url, SaveMode.Overwrite);
		/*System.out.println("===========================");
		for (Map<String, String> map : teenagerNames) {
			for (Iterator<String> iterator = map.keySet().iterator(); iterator.hasNext();) {
				key = iterator.next();
				System.out.println(key + ": " + map.get(key));
			}
			System.out.println("===========================");
		}*/
		
		ctx.stop();
	}

	public static void main(String[] args) {
		//new SparkSql().thriftServer();
		new SparkSql().teenager();
	}
	
}
