package org.apache.zookeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Locker {
	
	private final String ROOT = "/locks";
	
	private final String HOST = "192.168.126.199:2181";
	
	public String lock(String name) {
		return this.lock(name, 100l, 30000l);
	}
	
	public String lock(String name, Long duration) {
		return this.lock(name, 100l, duration);
	}
	
	public String lock(String name, Long interval, Long duration) {
		String result = null;
		
		try{
			long last = 0;
			do {
				result = this.getLock(name);
				if (result == null) {
					if (last >= duration) {
						System.out.println("can not get lock.");
						break;
					} else {
						last += interval;
					}
					
					Thread.sleep(interval);
				} else {
					break;
				}
			} while (result == null);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public void unlock(String lock) {
		ZooKeeper zk = this.connect(HOST);
		try {
			Stat stat = zk.exists(lock, false);
			if (stat != null) {
				stat = zk.exists(lock, false);
				if (stat != null) {
					zk.delete(lock, -1);
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			this.disConnect(zk);
		}
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
	
	private String getLock(String name) {
		String result = null;
		
		ZooKeeper zk = this.connect(HOST);
		try {
			Stat stat = zk.exists(ROOT, true);
			if (stat == null) {
				zk.create(ROOT, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
			String lock = zk.create(ROOT + "/" + name + "_", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			List<String> list = zk.getChildren(ROOT, false);
			if (list != null && !list.isEmpty()) {
				Collections.sort(list);
				String current = list.get(0);
				if (lock.equals(current)) {
					result = lock;
				} else {
					result = null;
				}
			} else {
				result = lock;
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			this.disConnect(zk);
		}
		
		return result;
	}
	
	public static void main(String[] args) {
		Locker locker = new Locker();
		String lock = locker.lock("test");
		System.out.println(lock);
		locker.unlock(lock);
	}

}
