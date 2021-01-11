package es.upm.dit.dscc.DHT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ServerManager {
	public List<Server> list_servers = new ArrayList<Server>();
	private static transient ZooKeeper zk;
	private transient static String rootServers = "/servers";
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};
	private static final int SESSION_TIMEOUT = 5000;
	
	private List<String> operative_servers = new ArrayList<String>();
	private Boolean relaunchServer = false;
	ServerManager(){
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				try {
					// Wait for creating the session. Use the object lock
					wait();
					Server.zk = zk;
					//zk.exists("/",false);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		} catch (Exception e) {
			System.out.println("Error initializing ZK");
		}
		
		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create a folder, if it is not created
				String response = new String();
				
				Stat s = zk.exists(rootServers, watcherMember); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootServers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					//System.out.println(response);
				}
				
				
				//Conseguimos los servidores ya creados
				List<String> list_child = zk.getChildren(rootServers, watcherMember, s); //this, s)
				
				
				
			} catch (KeeperException e) {
				System.out.println(e);
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

		}
	}
	
	
	public void createServer(){
		Integer index = getMissingIndex();
		Server _s = new Server(index);
		list_servers.add(_s);
	}
	
	private transient Watcher cWatcher = new Watcher() {
		public void process (WatchedEvent e) { 
			System.out.println("Created session");
			System.out.println(e.toString());
			notify();
		}
	};
	
	private transient Watcher  watcherMember = new Watcher() {
		public synchronized void process(WatchedEvent event) {
			try {
				System.out.println("------------------Watcher Member------------------\n");	
				List<String> servers = zk.getChildren(rootServers,  watcherMember);
				System.out.println("El número de servers en Zookeeper es: "+ servers.size());
				if (servers.size() > operative_servers.size()) {
					//El numero de servidores es mayor -> servidor creado.
					System.out.println("Lista de servidores activos actualizada.");
					operative_servers = servers;
				}
				else if (servers.size() < operative_servers.size()) {

				 	
					//El numero de servidores es menor -> servidor borrado
					//Obtenemos que servidor ha sido borrado
					
					for (int j=0; j<list_servers.size();j++) {
						Boolean found = false;
						for (int i=0; i<servers.size();i++) {
							//Indice del servidor
							String op_s_id = servers.get(i);
							
							if (list_servers.get(j).myId.equals(op_s_id)){
								//Este servidor lo tenemos
								found = true;
								
								break;
							}
						
						}
						if (!found) {
							Server dropped_s = list_servers.get(j);
							//Este servidor ya no está operativo, borramos el objeto
							System.out.println("El servidor con index "+dropped_s.myIndex+" se ha caido o ha sido eliminado.");
							list_servers.remove(dropped_s);
							dropped_s = null;

						}
					}
					if (!relaunchServer) {return;}
					//Obtenemos el index que falta por crear
					Integer missIndex = getMissingIndex();
					if(missIndex == 0) { //Todos los indices creados
						missIndex =  (int) Math.random() * 3 + 1;
						System.out.println("Todos los servidores han sido creados.");
					}
					System.out.println("Creando servidor con indice: "+missIndex);
					
					Server _s = new Server(missIndex);
					list_servers.add(_s);
					
					System.out.println("Recuperando datos...");
					
					Integer DHTidx = (missIndex>1)?missIndex-1: 3; //Si el idx es 1 -> La DHT se coge del S3, si no del Sidx-1
					Integer DHTReplicaidx = (missIndex<3)?missIndex+1: 1; //Si el idx es 3 -> La DHTReplica se coge del S1, si no del Sidx+1
					System.out.println("Objetiendo datos de las tablas...");
					_s.DHTTable = getTableData(DHTidx, true);
					_s.DHTTableReplica = getTableData(DHTReplicaidx, false);
					System.out.println("Servidor recuperado.");
				}
				else {
					System.out.println("La lista es igual");
					//El numero de servidores es igual -> ni idea de cuando pasa esto, supongo que si se borra y se crea a la vez (?)
				}
			} 
			catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} //this);
			}
			
	};
	
	
	private Integer getMissingIndex() {
		Integer[] ideal_index = {1,2,3};
		List<Integer> index_list = new ArrayList<Integer>();
		if (!list_servers.isEmpty()) {
			for (int j=0; j<list_servers.size();j++) {
				//Indice del servidor
				Server _s = list_servers.get(j);
				if(_s != null) {
					int _index_int = _s.myIndex;
					//Lo añadimos en la lista
					index_list.add(_index_int);
				}
				
			}
		}
		for (int id : ideal_index) { 
            if(!index_list.contains(id)) {
            	//Si no encuentra el index (1,2 o 3) dentro de los indices que tienen los servidores -> devuelve ese indeice
            	return id;
            }
        }
		//Si todos los indices están en los servidores

		return 0;
	}
	
	public HashMap<Integer,Integer> getTableData(Integer index, Boolean Replica){
		HashMap<Integer,Integer> table = null;
		for (int j=0; j<list_servers.size();j++) {
			Server _s = list_servers.get(j);
			if (_s.myIndex == index) {
				System.out.println("El servidor con index: "+index + " es: "+_s.myId);
				
				table = (Replica)?_s.DHTTableReplica:_s.DHTTable;
				return table;
			}
		}
		return table;
	}
	
	public void removeServer(Integer idx) throws InterruptedException, KeeperException {
		String id = "";
		for(Integer i=0;i<list_servers.size();i++) {
			Server _s = list_servers.get(i);
			System.out.println("Checking servers... - " +(i+1)+" of "+list_servers.size());
			if(_s.myIndex.equals(idx)) {

				id = _s.myId;
				System.out.println("Deleting znode with identifier: "+id);
				zk.delete(rootServers+"/"+id,-1);	
				return;
			}
		}
		System.out.println("The server with index: "+idx+ "was not found.");
		
	}
	
	public void printActiveServers() throws KeeperException, InterruptedException {
		System.out.println("------- Active servers -------");

		for (int i=0;i<list_servers.size();i++) {
			Server _s = list_servers.get(i);
			System.out.println((i+1)+") Server with name: "+ _s.myId+ " and index: "+_s.myIndex);
		}
	}
	
	public void toggleRelaunchServer() {
		this.relaunchServer = !this.relaunchServer;
		String status = (this.relaunchServer) ? "activada" : "desactivada";
		System.out.println("La recuperación automática está: " + status+ ".");
	}
	
	public void printTables() {
		
		
		for (int i=0;i<list_servers.size();i++) {
			Server _s = list_servers.get(i);
			System.out.println("Valores en DHTTable para el servidor: " + _s.myId + " para el index: "+ _s.myIndex);
			System.out.println("---------------------------------");
			System.out.println("|	KEY	|	VALUE	|");
			System.out.println("---------------------------------");
			for (Map.Entry<Integer, Integer> entry : _s.DHTTable.entrySet()) {
			    Integer key = entry.getKey();
			    Integer value = entry.getValue();
			    System.out.println("|	" + key + "	|	" + value + "	|");
		        
			    
			}
		    System.out.println("---------------------------------");
		    System.out.println();
			System.out.println("Valores en DHTTableReplica");
	
			System.out.println("---------------------------------");
			System.out.println("|	KEY	|	VALUE	|");
			System.out.println("---------------------------------");
			for (Map.Entry<Integer, Integer> entry : _s.DHTTableReplica.entrySet()) {
			    Integer key = entry.getKey();
			    Integer value = entry.getValue();
			    System.out.println("|	" + key + "	|	" + value + "	|");
		        
			    
			}
		    System.out.println("---------------------------------");
		    System.out.println();
		}
	}
	
	
	public static void main(String[] args) throws InterruptedException {
			
	
		boolean correct = false;
		int     menuKey = 0;
		boolean exit    = false;
		Scanner sc      = new Scanner(System.in);
	
	 
		Integer   key    = null;
	
	
		ServerManager s_manager = new ServerManager();
	
		
		TimeUnit.SECONDS.sleep(1);
		while (!exit) {
			try {
				correct = false;
				menuKey = 0;
				while (!correct) {
					System.out.println();
					System.out.println(">>> Enter option:\n 1) Create servers.\n 2) Remove random server.\n 3) Remove server by index.\n 4) Print active servers. \n 5) Activate/Deactivate auto-recovery. \n 6) Print tables. \n 0) Exit. \n");				
					if (sc.hasNextInt()) {
						menuKey = sc.nextInt();
						correct = true;
						Random rand = new Random();
						
					} else {
						sc.next();
						System.out.println("The provised text provided is not an integer");
					}
					
				}
				/*
				if(!dht.isQuorum) {
					System.out.println("No hay quorum. No es posible ejecutar su elección");
					continue;
				}
				*/
				switch (menuKey) {
				case 1:// Create	
	
					s_manager.createServer();
					//System.out.println("Ejecutado por " + dht.list_servers.get(int_random));
					break;
					
				case 2: // Remove random
					Random rand = new Random();
					Integer r_int = rand.nextInt(s_manager.list_servers.size())+1;
					System.out.println("Eliminando servidor con el índice: "+ r_int);
					s_manager.removeServer(r_int);
	
					break;
					
				case 3: // Remove index
					System.out.println("Indica el índice a eliminar");
					if (sc.hasNextInt()) {
						key = sc.nextInt();
						System.out.println("Eliminando servidor con el índice: "+ key);
						s_manager.removeServer(key);
						
						
					} else {
						sc.next();
						System.out.println("Tienes que indicar un número entero para el índice.");
					}
					break;
					
				case 4: //Print servers
					s_manager.printActiveServers();
					break;
				
				case 5:
					s_manager.toggleRelaunchServer();
					break;
				case 6:
					s_manager.printTables();
					break;
				case 0:
					exit = true;	
					//dht.close();
				default:
					break;
				
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

