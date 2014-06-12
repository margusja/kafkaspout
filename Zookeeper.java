package com.deciderlab.kafka;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.logging.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class Zookeeper {
	
	private final static Logger LOGGER = Logger.getLogger(Zookeeper.class.getName()); 
	
	/** 
	 * Returns znode(holds kafka consumers offset) data
	 * 
	 * @author margus@roo.ee
	 * @param connectString zkHost:port[,zkHost:port:...]
	 * @param sessionTimeout session timeout in milliseconds
	 * @param zkNodePath path to znode
	 * @return offset
	 */
	public static long getKafkaOffset(String connectString, int sessionTimeout, String zkNodePath)
	{
		long offset = -1;
		
		try {
			
			ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, null);
			Stat stat = null;
			try {
				byte[] b = zk.getData(zkNodePath, false, stat);
				 String s = new String(b);
				 //System.out.println(s);
				 
				 offset = Long.valueOf(s);
				 return offset;
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return offset;
	}
	
	
	/** 
	 * Sets Kafka topic-partation offset to zookeeper znode
	 * @author margus@roo.ee
	 * @param connectString zkHost:port[,zkHost:port:...]
	 * @param sessionTimeout session timeout in milliseconds
	 * @param zkNodePath path to znode
	 * @param newOffset new offset to set in zookeeper znode
	 * @return void
	 * 
	 */
	public static void setKafkaOffset (String connectString, int sessionTimeout, String zkNodePath, long newOffset)
	{
		
		LOGGER.info("got offset: "+ newOffset);
		
		ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
		
		String s = String.valueOf(newOffset);
		char arr[]=s.toCharArray();
		for(int i=0;i<arr.length;i++){
		    //System.out.println("Data at ["+i+"]="+arr[i]);
		    bOutput.write(arr[i]);
		}
		byte b [] = bOutput.toByteArray();
		
		
		ZooKeeper zk;
		try {
			zk = new ZooKeeper(connectString, sessionTimeout, null);
			
			
			try {
				zk.setData(zkNodePath, b, -1);
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}
