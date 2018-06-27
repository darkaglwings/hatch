package org.apache.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Barrier {
	
	private final String ROOT = "/barrier";
	
	private final String COMPLETED = "/barrier/completed";
	
	public void producer(String host, String node, String value, String... datas) {
		ZooKeeper zk = this.connect(host, new Watcher() {
			public void process(WatchedEvent event) {
				new Barrier().trigger(host, ROOT, datas);
			}
		});
		
		if (zk != null) {
			try {
				Stat stat = zk.exists(ROOT, false);
				if (stat == null) {
					zk.create(ROOT, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				
				zk.create(ROOT + "/" + node, value.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
	
	public void trigger(String host, String node, String... datas) {
		ZooKeeper zk = this.connect(host, new Watcher() {
			public void process(WatchedEvent event) {
				/*System.out.println("type: " + event.getType());
				System.out.println("path: " + event.getPath());*/
			}
		});
		
		try {
			Stat stat = zk.exists(COMPLETED, true);
			if (stat == null) {
				zk.create(COMPLETED, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
			stat = zk.exists(node, true);
			if (stat != null) {
				List<String> list = zk.getChildren(node, false);
				boolean isCompleted = true;
				for (String data : datas) {
					if (!list.contains(data)) {
						isCompleted = false;
						break;
					}
				}
				
				if (isCompleted) {
					zk.create(COMPLETED + node, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
	
	private ZooKeeper connect(String host, Watcher watcher) {
		ZooKeeper zk = null;
		try {
			zk = new ZooKeeper(host, 60000, watcher);
		} catch (IOException e) {
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

	public static void main(String[] args) {
		String host = "192.168.126.199";
		
		Barrier barrier = new Barrier();
		barrier.producer(host, "A", "aaa", "A", "B");
		barrier.producer(host, "B", "bbb", "A", "B");
		
		ZooKeeper zk = barrier.connect(host, new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("type: " + event.getType());
				System.out.println("path: " + event.getPath());
				
			}
		});
		
		
		try {
			Stat stat = null;
			int i = 0;
			do {
				stat = zk.exists("/barrier/completed/barrier", true);
				System.out.println(i++ + ":" + stat);
			} while (stat == null);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
