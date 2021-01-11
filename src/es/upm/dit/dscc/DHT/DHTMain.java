package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
//import java.util.logging.Filter;
//import java.util.logging.Handler;
import java.util.logging.Level;
//import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper; 
import org.apache.zookeeper.data.Stat;
import java.util.Iterator;

public class DHTMain{

	private Integer Quorum = 2;
	public Boolean isQuorum = false;

	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};
	private ZooKeeper zk;
	private static String rootUsers = "/users";
	private static String aUser = "/User-";
	private static String rootServers = "/servers";
	private String userId;
	private static final int SESSION_TIMEOUT = 5000;
	public List<String> list_servers;
	public Integer outputMutex = -1;
	public Integer initMutex = -1;
	public DHTMain() {
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);
		System.out.println("Hosts: "+i);
		//this.myIndex;
		//this.minIndexValue = minIndexValue;
		//this.maxIndexValue = maxIndexValue;
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, watcherMember);
				try {
					// Wait for creating the session. Use the object lock
					wait();
					//zk.exists("/",false);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		} catch (Exception e) {
			System.out.println("Error");
		}
		
		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create a folder, if it is not created
				String response = new String();
				
				Stat s = zk.exists(rootUsers, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootUsers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}
				
				Stat s2 = zk.exists(rootServers, watcherMember); //this);
				if (s2 == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootServers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}
				//Conseguimos los servidores ya creados
				List<String> list_child = zk.getChildren(rootServers, watcherMember, s);

				this.list_servers = list_child;
				this.isQuorum = (this.Quorum <= this.list_servers.size()) ? true : false; //comprobamos si hay quorum
				this.userId = zk.create(rootUsers + aUser, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
				this.userId = this.userId.replace(rootUsers + "/", "");
				//Y creamos el watcher a nosotros mismos
				String userPath = rootUsers+"/"+userId;
				System.out.println(userPath);
				zk.getChildren(userPath,  selfWatcherUser, s); //this);

				//printListOperations(list2);
			} catch (KeeperException e) {
				System.out.println(e);
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

		}
		//Se crea un nodo donde se alojan todos los mensajes que va a mostrar, al mostrarlo se eliminan ole ole
	}
	
	private Watcher  watcherMember = new Watcher() {
		public void process(WatchedEvent event) {	
			try {
				List<String> list = zk.getChildren(rootServers,  watcherMember); //this);
				list_servers = list;
				isQuorum = (Quorum <= list_servers.size()) ? true : false; //comprobamos si hay quorum
			} 
			
			catch (Exception e) {
				System.out.println("Exception: wacherMember");
			}
		}
	};
	private Watcher selfWatcherUser = new Watcher() {
		public void process(WatchedEvent event) {	
			try {
				//Coge el primer elemento de la lista, lo muestra y lo elimina
				String userPath = rootUsers+"/"+userId;
				List<String> user_messages = zk.getChildren(userPath,  selfWatcherUser); //this);
				if(user_messages.size() == 0) {return;} //Para cuando se borran y vuelve aqui, -.-
				byte[] _data= zk.getData(userPath+"/"+user_messages.get(0),false,new Stat());
				Message msg = deserializeMessage(_data);
				switch(msg.getOperation()) {
				case PUT_MAP:
					System.out.println("Se ha guardado el valor: "+ msg.getValue());
					break;
					
				case GET_MAP:
					if(msg.getValue() != null) {
						System.out.println("Para la key "+msg.getKey()+" se ha devuelto el valor: "+ msg.getValue());
					}
					else {
						System.out.println(msg.getKey());
						System.out.println("No se ha encontrado el valor");
					}
					break;
					
				case REMOVE_MAP:
					System.out.println("Se ha borrado el campo: " + msg.getKey() + "con valor: " + msg.getValue());
					break;
					
				default:
					System.out.println("No se ha reconocido el tipo de operacion");
				}
				synchronized (outputMutex) {
					outputMutex.notify();
				}

				
				//Eliminamos el mensaje
				zk.delete(userPath+"/"+user_messages.get(0),-1);
			} 
			
			catch (Exception e) {
				System.out.println("Exception: selfWatcherUser");
				e.printStackTrace();
			}
		}
	};

	public Message deserializeMessage(byte[] data) {
		Message msg = null;
		if (data != null) {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(data);
				ObjectInputStream     in = new ObjectInputStream(bis);
				
				msg  = (Message) in.readObject();
			}
			catch (Exception e) {
				System.out.println("Error while deserializing object");
			}
		}

		return msg;
	}
	public void initMembers() {
		Map<String,Integer> init_values = new HashMap<String,Integer>();
		//Inicializamos el Map
		init_values.put("Angel", 1);
		init_values.put("Bernardo", 2);
		init_values.put("Carlos", 3);
		init_values.put("Daniel", 4);
		init_values.put("Eugenio", 5);
		init_values.put("Zamorano", 6);
		
		int int_random = 0;
		
		for (Map.Entry<String, Integer> entry : init_values.entrySet()) {
			Random rand = new Random();
			int_random = rand.nextInt(list_servers.size());
			DHT_Map dht_map = new DHT_Map(entry.getKey(), entry.getValue());
			OperationZookeeper operacion = new OperationZookeeper();
			processOperation(operacion,dht_map,OperationEnum.PUT_MAP,null,null,list_servers.get(int_random));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
 
	}

	//////////////////////////////////////////////////////////////////////////

	public DHT_Map putMap(Scanner sc) {
		String  key     = null;
		Integer value   = 0;

		System. out .print(">>> Enter name (String) = ");
		key = sc.next();


		System. out .print(">>> Enter account number (int) = ");
		if (sc.hasNextInt()) {
			value = sc.nextInt();
		} else {
			System.out.println("The provised text provided is not an integer");
			sc.next();
			return null;
		}

		return new DHT_Map(key, value);
	}
	
	public Integer processOperation(OperationZookeeper operacion,DHT_Map map, OperationEnum _oper_enum,Integer _value,String _key,String _server_id) {
		try{
			OperationsDHT operation;
			if(_oper_enum == OperationEnum.PUT_MAP) {
				operation = new OperationsDHT(_oper_enum,map.getValue(),map.getKey(),_server_id,this.userId);
			}
			else {
				operation = new OperationsDHT(_oper_enum,_value,_key,_server_id,this.userId);
			}
		
			
			operacion.createNodeOperation();
			operacion.putData(operation, operacion.Id);
			return 200; //Tipico codigo de OK
		}
		catch(Exception e) {
			System.out.println(e);
			return -1; //Error
		}
	}


	//////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws InterruptedException {
		

		boolean correct = false;
		int     menuKey = 0;
		boolean exit    = false;
		Scanner sc      = new Scanner(System.in);
		String cluster  = null;

 
		String   key    = null;
		Integer value   = 0;

		if (args.length == 0) {
			System.out.println("Incorrect arguments: dht.Main <Number_Group>");
			//sc.close();
			//return;
		} else {
			cluster = args[0];
		} 

		DHTMain dht = new DHTMain();
		

		TimeUnit.SECONDS.sleep(1);
		while (!exit) {
			int int_random = 0;
			try {
				correct = false;
				menuKey = 0;
				if(!dht.isQuorum) {
					System.out.println("No hay quorum. No es posible ejecutar su elección");
					continue;
				}
				while (!correct) {
					System.out.println(">>> Enter option: 1) Put. 2) Get. 3) Reomve. 0) Exit");				
					if (sc.hasNextInt()) {
						menuKey = sc.nextInt();
						correct = true;
						Random rand = new Random();
						int_random = rand.nextInt(dht.list_servers.size());
						
					} else {
						sc.next();
						System.out.println("The provised text provided is not an integer");
					}
					
				}

				
				OperationZookeeper operacion = new OperationZookeeper();
				switch (menuKey) {
				case 1:// Put	

					DHT_Map map = dht.putMap(sc);
					if(dht.processOperation(operacion, map,OperationEnum.PUT_MAP,value,key,dht.list_servers.get(int_random)) == 200) {
						System.out.println("Operación procesada");
					}
					//System.out.println("Ejecutado por " + dht.list_servers.get(int_random));
					break;
					
				case 2: // Get
					System. out .print(">>> Enter key (String) = ");
					
					key    = sc.next();
					if(dht.processOperation(operacion,null,OperationEnum.GET_MAP,null,key,dht.list_servers.get(int_random)) == 200) {
						System.out.println("Operación procesada");
					}

					break;
					
				case 3: // Remove
					System. out .print(">>> Enter key (String) = ");
					key    = sc.next();
					
					if(dht.processOperation(operacion,null,OperationEnum.REMOVE_MAP,null,key,dht.list_servers.get(int_random)) == 200) {
						System.out.println("Operación procesada");
					}

					break;

				case 0:
					exit = true;	
					//dht.close();
				default:
					break;
				
				}
				synchronized (dht.outputMutex) {
					dht.outputMutex.wait();
				}
			//}
			} catch (Exception e) {
				System.out.println("Exception at Main. Error read data");
				System.err.println(e);
				e.printStackTrace();
			}

		}

		sc.close();
	}
	
}
