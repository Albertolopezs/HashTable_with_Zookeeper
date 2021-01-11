package es.upm.dit.dscc.DHT;


import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream; 
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class OperationZookeeper {

	private ZooKeeper zk = null;
	private String operationPath = "/operations";
	private String operationNodePath = "/operation-";
	private Integer mutex        = -1;
	private static final int SESSION_TIMEOUT = 5000;
	public String Id;

	public OperationZookeeper(){

		// This is static. A list of zookeeper can be provided for decide where to connect
		String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create the session
		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		try {
			if (zk == null) {
				// No need for watchers.
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, null);
				// We initialize the mutex Integer just after creating ZK.
				try {
					// Wait for creating the session. Use the object lock
					//synchronized(mutex) {
					//	mutex.wait();
					//}
					//zk.exists("/", false);
				} catch (Exception e) {
					System.out.println("Exception in constructor");
				}
			}
		} catch (Exception e) {
			System.out.println("Exception in constructor");
		}

		// Add the process to the members in zookeeper


	}
	
	public void createNodeOperation() {
		if (zk != null) {
			try {
				// Create a folder, if it is not created
				String response = new String();
				
				Stat s = zk.exists(operationPath, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(operationPath, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}

				// Create a znode for registering as member and get my id
				String myId = zk.create(operationPath + operationNodePath, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

				myId = myId.replace(operationPath + "/", ""); //Coge todo el path y le quita el prefijo
				Id = myId;
			} catch (KeeperException e) {
				System.out.println(e);
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
	}


	public void putData(OperationsDHT operation, String myId) {

		byte [] data = null;;
		int version = -1;

		// Serialize: Convert an operation in an array of Bytes.
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(operation);
			oos.flush();
			data = bos.toByteArray();
		} catch (Exception e) {
			System.out.println("Error while serializing object");
		}

		// Put the data in a zNode
		try {
			zk.setData(operationPath +"/"+myId, data, version);			
		} catch (Exception e) {
			System.out.println(e); 
			System.out.println("Error while setting data in ZK");
		}

	}

	public OperationsDHT getData(String myId) {
		Stat s = new Stat();
		byte[] data = null;
		OperationsDHT operation = null;
		
		// Get the data from a zNode
		try {
			data = zk.getData( myId, false, s);
			s = zk.exists(myId, false);	
		} catch (Exception e) {
			System.out.println(e);
			System.out.println("Error while getting data from ZK");
		}
		
		// Deserialize: Convert an array of Bytes in an operation.
		if (data != null) {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(data);
				ObjectInputStream     in = new ObjectInputStream(bis);
				
				operation  = (OperationsDHT) in.readObject();
			}
			catch (Exception e) {
				System.out.println("Error while deserializing object");
			}
		}

		return operation;
	}

}