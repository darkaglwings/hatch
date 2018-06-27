package org.apache.zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Queue {
	
	private final String ROOT = "/queue";
	
	public void producer(String host, String value) {
		ZooKeeper zk = this.connect(host);
		if (zk != null) {
			try {
				Stat stat = zk.exists(ROOT, true);
				if (stat == null) {
					zk.create(ROOT, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				zk.create(ROOT + "/", value.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				this.disConnect(zk);
			}
		} else {
			System.err.println("can not connect to host: " + host);
		}
	}
	
	public String consumer(String host) {
		String result = null;
		
		ZooKeeper zk = this.connect(host);
		try {
			Stat stat = zk.exists(ROOT, true);
			if (stat != null) {
				synchronized (this) {
					List<String> list = zk.getChildren(ROOT, true);
					if (list != null && !list.isEmpty()) {
						Collections.sort(list);
						result = new String(zk.getData(ROOT + "/" + list.get(0), true, stat), "UTF-8");
						zk.delete(ROOT + "/" + list.get(0), -1);
					}
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
			this.disConnect(zk);
		}
		
		return result;
	}
	
	private ZooKeeper connect(String host) {
		ZooKeeper zk;
		try {
			zk = new ZooKeeper(host, 60000, new Watcher(){
				public void process(org.apache.zookeeper.WatchedEvent event) {
					System.out.println(event.toString());
				}
			});
		} catch (IOException e) {
			zk = null;
			e.printStackTrace();
		}
		
		return zk;
	}
	
	private void disConnect(ZooKeeper zk) {
		try {
			zk.close();
		} catch (InterruptedException e) {
			zk = null;
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unused")
	private String cluster(String... hosts) {
		String result = "";
		for (String host : hosts) {
			result += host + ",";
		}
		result = result.substring(0, result.length() - 1);
		return result;
	}
	
	public static void main(String[] args) {
		String host1 = "192.168.126.199:2181";
		String host2 = "192.168.126.200:2181";
		String host3 = "192.168.126.201:2181";
		
		String message = "aaa";
		
		Queue queue = new Queue();
		queue.producer(host1, message);
		//System.out.println(queue.consumer(host1));
	}

}
