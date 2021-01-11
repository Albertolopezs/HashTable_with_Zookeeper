package es.upm.dit.dscc.DHT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.Serializable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;



import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.Watcher.Event.EventType;

public class Server extends Thread implements Watcher,StatCallback,Serializable {

	private static final int SESSION_TIMEOUT = 5000;
	//private int serialVersionUID = 1; //Version 1 en formato long
	//La etiqueta transient hace que no se serialize
	private transient static String rootServers = "/servers";
	private transient static String aServer = "/Server-";
	private transient String operationPath = "/operations";
	public HashMap<Integer, Integer> DHTTable = new HashMap<Integer, Integer>();
	public HashMap<Integer, Integer> DHTTableReplica = new HashMap<Integer, Integer>();
	public transient List<String> prevStateChilds;
	public String myId;
	public Integer myIndex;
	private transient Integer minIndexValue;
	private transient Integer maxIndexValue;
	private transient Integer minIndexReplicaValue;
	private transient Integer maxIndexReplicaValue;
	private Integer Operations = 0; //Numero de operaciones "observadas"
	private transient Integer OperationMutex = -1;
	private Boolean SavedState = false; 
	private Boolean initialized = false;
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};
	
	public static transient ZooKeeper zk;
	
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format",
				"[%1$tF %1$tT][%4$-7s] [%5$s] [%2$-7s] %n");

		//    		"[%1$tF %1$tT] [%2$-7s] %3$s %n");
		//           "[%1$tF %1$tT] [%4$-7s] %5$s %n");
		//   "%4$s: %5$s [%1$tc]%n");
		//    "%1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS %1$Tp %2$s%n%4$s: %5$s%n");
	}

	static final Logger LOGGER = Logger.getLogger(DHTMain.class.getName());

	public Server(Integer index) {
		// TODO Auto-generated constructor stub
		configureLogger();
		
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);
		//this.myIndex;
		//this.minIndexValue = minIndexValue;
		//this.maxIndexValue = maxIndexValue;
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
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
				
				Stat s = zk.exists(rootServers, false); //this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootServers, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					//System.out.println(response);
				}
				
				Stat s2 = zk.exists(operationPath, watcherOperation); //this);
				if (s2 == null) {
					// Created the znode, if it is not created.
					response = zk.create(operationPath, new byte[0], 
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					//System.out.println(response);
				}
				//Conseguimos los servidores ya creados
				List<String> list_child = zk.getChildren(rootServers, false, s); //this, s)
				this.prevStateChilds = list_child;
				this.myIndex = index;
				if(this.myIndex == 0) { //Todos los indices creados
					this.myIndex =  (int) Math.random() * 3 + 1;
					System.out.println("Todos los servidores han sido creados, creando replica con índice "+this.myIndex);
				}
				System.out.println("-- Info -- Creado servidor con index: "+ this.myIndex);
				//Con el indice que nos corresponde sacamos el rango de valores a guardar
				int segment = Integer.MAX_VALUE / 3; // No negatives
				//Limites del servidor
				this.minIndexValue = (this.myIndex-1)* segment;
				this.maxIndexValue = (this.myIndex)* segment;
				//Limite de la replica
				this.minIndexReplicaValue = (this.myIndex<3)?(this.myIndex)* segment: 0;
				this.maxIndexReplicaValue = (this.myIndex<3)?(this.myIndex+1)* segment: segment;
				// Create a znode for registering as member and get my id
				this.myId = zk.create(rootServers + aServer, new byte[0], 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				this.myId = myId.replace(rootServers + "/", "");
				//Guardamos el estado del servidor en ZK
				putSerializedServer(-1);
				//Creamos un watcher para nosotros mismos
				

				zk.getChildren(operationPath, watcherOperation, s2); //this, s);
				System.out.println("Created znode nember id:"+ myId );
				//inicializado
				initialized = true;
				//printListOperations(list2);
			} catch (KeeperException e) {
				System.out.println(e);
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

		}
	}
	
	

	private transient Watcher cWatcher = new Watcher() {
		public void process (WatchedEvent e) { 
			System.out.println("Created session");
			System.out.println(e.toString());
			notify();
		}
	};


	private transient Watcher  watcherOperation = new Watcher() {
		public synchronized void process(WatchedEvent event) {
			//System.out.println("------------------Watcher Operation------------------\n");	
			//¿Que se va a hacer cada vez que se detecte una operación?
			/*
			 * -Obtenerla, getData()
			 * - Ver si nos afecta
			 * - Si nos afecta modificar la tabla
			 * - Cuando se termine se añade un nodo con nuestro identificador como hijo de la operación
			 * 
			 * ¿QUE PASA CON EL SERVIDOR QUE HA CREADO LA OPERACION?
			 * Tambien recibe un watcher
			 * Se crea un watcher para los hijos de la operación recibida (event.getPath())
			 * 
			 * ¿Que pasa si hay operaciones mas viejas a la espera?
			 * sleep/wait hasta que event == operacion.getChildren[0] (es decir, el primero
			 */
			String nodePath = "";
			OperationsDHT operation;
			String eventPath = event.getPath(); //Esto nos da el path del node padre (mierdon)
			Integer keyHash;
			
			try {
				wait(1000);
				//System.out.println(event.getType());
				List<String> list = zk.getChildren(eventPath, false); //this, s);
				if(Operations >= list.size() && Operations >0) {
					//Se ha eliminado 1 nodo, que ha sido procesado asi que Lo eliminamos de Operations
					Operations -= 1;
					zk.getChildren(eventPath, watcherOperation); //this, s);
					return;
				}				
				nodePath = list.get(Operations); //-Obtenerla, a partir de su index
		
			}
			catch(Exception e) {
				System.out.println("Error en el watcher Operation "+e);
				return;
			}
			
			try {
				OperationZookeeper OZK = new OperationZookeeper();
				operation = OZK.getData(eventPath+"/"+nodePath); //Operación asociada a ese nodo
				}
				
			
			catch (Exception e) {
				System.out.println("Error descargando los datos "+e);
				return;
			}
			
			try { 
				
				//Calcular el Hash y a que servidor esta destinado
				//cada servidor tiene su tabla y la replica. 
				//miraria cuales son los servidores up, si el servidor al que  esta destinado no existe, tiene que contestar la replica
				//List<String> listaServidoresUp = zk.getChildren(rootServers,  false); //this);
				keyHash = operation.getKey().hashCode();
				keyHash = (keyHash>0)?keyHash:-keyHash;
				Message msg = null; 
				if(minIndexValue<=keyHash && keyHash<maxIndexValue) {
					//DHTTable propia
					msg = handleOperations(operation,keyHash,DHTTable);
				}
				else if(minIndexReplicaValue<=keyHash && keyHash<maxIndexReplicaValue) {
					//DHTTable Replica
					msg = handleOperations( operation,keyHash,DHTTableReplica);
				}
				byte[] data = (msg != null) ? serializeMessage(msg):new byte[0]; //Si hay un mensaje (el servidor tiene informacion) se añade a la operacion
				
				

				//Configuramos el watcher de hijos para el nodo de la ooperacion (OperationsDoneWatcher)
				zk.getChildren(eventPath+"/"+nodePath,  watcherOperationDone);	
				//Se crea como hijo de la operación un nodo con la id del servidor
				
				zk.create(eventPath+"/"+nodePath +"/"+ myId,data, 
						Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				
				List<String> list = zk.getChildren(eventPath,  watcherOperation);
				
				//Aumentamos en 1 el numero de Operations observed
				Operations  +=1;
			}
				catch (Exception e) {
					System.out.println("Error procesando los datos "+e);
					e.printStackTrace();
					return;
				}
			
			notify();
		}
	};
	private transient Watcher  watcherOperationDone = new Watcher() {
		public synchronized void process(WatchedEvent event) {
			/*
			 * ¿QUE PASA CON EL SERVIDOR QUE HA CREADO LA OPERACION?
			 * Este solo se ejecuta en el servidor que sube una operación
			 * Se crea un watcher cada vez que un servidor procesa una operación
			 * Si operations.get_children.length < members.get_children.length --> Hay servidores sin procesar
			 * Else --> Todos lo han hecho, borramos el nodo de operación
			 */
			/*
			 */
			//System.out.println("------------------Watcher Operation Checked------------------\n");	
			String eventPath = event.getPath(); //Esto nos da el path de la ooperacion
			try {
				
			}
			catch(Exception e) {
				//System.out.println("Error comparando los hijos "+e);
				return;
			}
			try {
				OperationZookeeper OZK = new OperationZookeeper();
				OperationsDHT operation = OZK.getData(eventPath); //Operación asociada a ese nodo
				//System.out.println("ID Operacion "+operation.getServerId()+" ID Mia "+myId);
				if(operation.getServerId().equals(myId)) {
					List<String> confirmedList = zk.getChildren(eventPath, false); //Hijos de la operacion que lo han ejecutado
					int executedServers = confirmedList.size(); //-Obtenerla, a partir de su index
					List<String> list2 = zk.getChildren(rootServers, false); //Numero de servidores activos
					int activeServers = list2.size();
					//System.out.println("Servers ejecutados "+executedServers+" Servers activos "+activeServers);
					while(executedServers < activeServers) {
						synchronized (OperationMutex) {
							OperationMutex.wait();
						}
						
						//Actualizamos los valores de executed y active
						confirmedList = zk.getChildren(eventPath, false); //Hijos de la operacion que lo han ejecutado
						executedServers = confirmedList.size(); //-Obtenerla, a partir de su index
						list2 = zk.getChildren(rootServers, false); //Numero de servidores activos
						
						activeServers = list2.size();
					}
					List<Message> msg_list = new ArrayList<Message>();
					//Borramos los hijos del nodo
					for (Iterator iterator = confirmedList.iterator(); iterator.hasNext();) {
						String childName = (String) iterator.next();
						//Obtenemos los datos y los guardamos si son diferentes dee byte[0]
						byte[] _data = zk.getData(eventPath+"/"+childName, false, new Stat());
						if(_data.length != 0) {
							msg_list.add(deserializeMessage(_data));
						}
						zk.delete(eventPath+"/"+childName,-1);			
					}
					//YO he creado esta operación
					//Tengo que borrarla
					zk.delete(eventPath,-1);
					//Como lo hemos borrado, dejamos de tenerlo en cuenta para las operaciones observadas
					
					//Ahora tenemos toda la informacion de todos los servers en msg_list, la procesamos y creamos un nodo en el user
					//System.out.println("Numero de servers que han dado información: "+msg_list.size());
					Message msg_to_show = selectMessage(msg_list);
					//Y lo añadimos como hijo al user
					String user_id = operation.getUserId();
					byte[] serialized_msg = serializeMessage(msg_to_show);
					String msg_id = zk.create("/users/"+user_id+"/",serialized_msg, 
							Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
					//System.out.println("Se ha creado el nodo en: "+"/users/"+user_id+"/ con id "+msg_id);
					//System.out.println("Se ha eliminado el nodo "+eventPath);
					
					//Como se ha hecho una opeeración -> SavedState = false
					SavedState = false;

					
				}
				else {
					synchronized (OperationMutex) {
						OperationMutex.notify();
					}
					SavedState = false;
				}
				}
				
			
				catch (Exception e) {
					System.out.println("Error descargando los datos "+e);
					return;
			}
			
			/*
			System.out.println("------------------Watcher Operation------------------\n");		
			try {
				System.out.println("        Update!!");
				List<String> list = zk.getChildren(operationPath,  watcherOperation); //this);
				printListMembers(list);
			} catch (Exception e) {
				System.out.println("Exception: wacherOperation");
			}
			*/
		}
	};
	
	public Message selectMessage(List<Message> msg_list) {
		//Esta funcion va a coger, de todos los mensajes, el que tenga la mitad o más (de 2 -> 1, si hay servidores replicados pues mejor)
		Message msg = msg_list.get(0);
		int cnt = 1; //msg ahora coincide consigo mismo, cnt = 1
		int min_coincidence_value =(int) msg_list.size()/2;
		for (int i=1;i<msg_list.size();i++) {
			if(		(msg.getOperation() == msg_list.get(i).getOperation()) &&
					(msg.getValue() == msg_list.get(i).getValue()) &&
					(msg.getKey() == msg_list.get(i).getKey())
					) {
				//Significa quee todos los campos son iguales, bizancioooooooo
				cnt +=1;
			}
			if(cnt >= min_coincidence_value) {
				return msg;
			}
		}
		//Si no se consigue esto para el valor 1, vamos a ver para el resto
		//Por que se puede eliminar? Pues porque no quitamos ningun caso de coincidencia, si msg(0) tuviese el caso bueno
		// ya nos hubiese dado en el primer bucle, cualquiera que sea como msg(0) no va a ser
		
		msg_list.remove(msg); 
		msg = selectMessage(msg_list);
		
		
		return msg;
	}
	
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}

	//////////////////////////////////////////////////////////////////////////

	public void configureLogger() {
		ConsoleHandler handler;
		handler = new ConsoleHandler(); 
		handler.setLevel(Level.FINE); 
		LOGGER.addHandler(handler); 
		LOGGER.setLevel(Level.FINE);
	}

	//////////////////////////////////////////////////////////////////////////
	
	///////////////////////////////OPERATION HANDLE /////////////////////////
	
	//Crea un nodo operación y lo añade al usuario
	public Message handleOperations(OperationsDHT operation, Integer keyHash,HashMap<Integer, Integer> Table) {
		//LOGGER.fine("Operation in a message: " + operation.getOperation());
		Integer value;
		boolean status;
		Message msg = null;
		switch (operation.getOperation()) {
		case PUT_MAP:
			//LOGGER.fine(operation.getOperation() + " Key: " + operation.getMap().getKey());
			putMsg(Table,keyHash,operation.getValue());
			System.out.println("El servidor: "+this.myId+ "ha guardado el valor: "+ DHTTable.get(keyHash));
			msg = new Message(OperationEnum.PUT_MAP,operation.getValue(),operation.getKey());
			
			//LOGGER.fine(operation.getOperation() + "Mesage sent");
			break;
		case GET_MAP:
			value = getMsg(Table,keyHash);
			System.out.println("El servidor: "+this.myId+ "ha devuelto el valor: "+ value);
			msg = new Message(OperationEnum.GET_MAP,value,operation.getKey());
			break;
		case REMOVE_MAP:
			value = removeMsg(Table,keyHash);
			System.out.println("El servidor: "+this.myId+ "ha borrado el campo: " + keyHash.hashCode() + "con valor: " + value);
			msg = new Message(OperationEnum.REMOVE_MAP,value,operation.getKey());
			break;
		/*
		case CONTAINS_KEY_MAP:
			status = dht.containsKey(operation.getKey());
			if (!operation.isReplica()) {
				sendMessages.returnStatus(address, status);
			}
			break;
		case KEY_SET_HM:
			//System.out.println(operation.getClientDB().toString());
			//System.out.println("------------------------------");
			//clientDB.createBank(operation.getClientDB());
			break;
		case VALUES_HM :
			break;

		case RETURN_VALUE :
			mutex.receiveOperation(operation);
			break;

		case RETURN_STATUS :
			mutex.receiveOperation(operation);
			break;

		case INIT:
			LOGGER.fine("Received INIT:" + operation.getPosReplica());
			//viewManager.putDHTServers(operation.getDHTServers());
			viewManager.transferData(address);
			break;
			
		case DHT_REPLICA :
			LOGGER.fine("Received servers (DHTServers):" + operation.getPosReplica());
			viewManager.putDHTServers(operation.getDHTServers());
			break;
			
		case DATA_REPLICA : //TODO: no se está usando
			LOGGER.fine("Received data replica:" + operation.getPosReplica());
			viewManager.putReplica(operation.getPosReplica(), operation.getDHT());
			break;
		*/
		default:
			break;
		}
		
		return msg;
	}
	
	public void putMsg(HashMap<Integer, Integer> Table, Integer key, Integer value) {
		Table.put(key,value);
	}
	public Integer getMsg(HashMap<Integer, Integer> Table,Integer key) {
		try {
			Integer value = Table.get(key);
			return value;
		}
		catch(Exception e) {
			System.out.println("El valor no ha sido encontrado");
		}
		return null;
	}
	public Integer removeMsg(HashMap<Integer, Integer> Table,Integer key) {
		try {
			Integer value = Table.remove(key);
			return value;
		}
		catch(Exception e) {
			System.out.println("El valor no ha sido encontrado o no ha podido ser borrado.");
		}
		return null;
	}

	@Override
	public void processResult(int arg0, String arg1, Object arg2, Stat arg3) {
		// TODO Auto-generated method stub
		
	}
	
	
	
	/* ---------------SERIALIZE--------------*/
	
	public byte[] serializeMessage(Message msg) {
		byte[] data = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(msg);
			oos.flush();
			data = bos.toByteArray();
			return data;
		} catch (Exception e) {
			System.out.println("Error while serializing object");
			return data;
			
		}
	}
	
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
				e.printStackTrace();
			}
			
		}

		return msg;
	}
	public void putSerializedServer(int version) {

		byte [] data = null;
		// Serialize: Convert an operation in an array of Bytes.
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(this);
			oos.flush();
			data = bos.toByteArray();
		} catch (Exception e) {
			System.out.println("Error while serializing object "+e);
		}

		// Put the data in a zNode
		try {
			zk.setData(rootServers +"/"+this.myId, data, version);			
		} catch (Exception e) {
			System.out.println(e);
			System.out.println("Error while setting data in ZK");
		}
		
	}

	public Server getSerializedServerWatcher (String myId) {
		Stat s = new Stat();
		byte[] data = null;
		Server server = null;
		
		// Get the data from a zNode
		try {
			data = zk.getData(rootServers +"/"+myId, false, s);
			//s = zk.exists(myId, false);	
		} catch (Exception e) {
			System.out.println(e);
			System.out.println("Error while getting data from ZK");
		}
		
		// Deserialize: Convert an array of Bytes in an operation.
		if (data != null) {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(data);
				ObjectInputStream     in = new ObjectInputStream(bis);
				
				server  = (Server) in.readObject();
			}
			
			catch(EOFException e) {
				System.out.println("El servidor no ha sido inicializado");
				return server;
			}
			catch (Exception e) {
				System.out.println("Error while deserializing object");
				e.printStackTrace();
			}
		}

		return server;	
	}
	public Server getSerializedServer(String myId) {
		Stat s = new Stat();
		byte[] data = null;
		Server server = null;
		System.out.println("-- Info -- Getting info from server: "+rootServers +"/"+myId);
		// Get the data from a zNode
		try {
			data = zk.getData(rootServers +"/"+myId, false, s);
			//s = zk.exists(myId, false);	
		} catch (Exception e) {
			System.out.println(e);
			System.out.println("Error while getting data from ZK");
		}
		
		// Deserialize: Convert an array of Bytes in an operation.
		if (data != null) {
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(data);
				ObjectInputStream     in = new ObjectInputStream(bis);
				
				server  = (Server) in.readObject();
			}
			
			catch(EOFException e) {
				System.out.println("El servidor no ha sido inicializado");
				return server;
			}
			catch (Exception e) {
				System.out.println("Error while deserializing object");
				e.printStackTrace();
			}
		}

		return server;
	}
	public static void main(String[] args) {
		/*
        Thread t = new Thread() {
        	public void run() {
        		Server _s = new Server();
        		while(true) {
        			// cada 3 segundos lanzamos recordatorio watchers
        			
        		}

        	}
        };
        t.start();
        */

	}
}
