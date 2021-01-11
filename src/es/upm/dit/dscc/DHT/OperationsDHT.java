package es.upm.dit.dscc.DHT;

import java.io.Serializable;
//import java.util.Set;
//import java.util.HashMap;

//import org.jgroups.Address;

public class OperationsDHT implements Serializable {

	private static final long serialVersionUID = 1L;
	private OperationEnum operation;
	private Integer     value         = null;       
	private String      key           = null;
	private String 		serverId;
	private String		userId;
	//private DHT_Map     map           = null;
	//private boolean     status        = false;
	//private boolean     isReplica     = false;
	//private int         posReplica;
	//private int         posServer;
	//private DHTUserInterface dht      = null;
	// private HashMap<Integer, Address> DHTServers;
	// private Set<String> 
	// private ArrayList<Integer>
	
	public OperationsDHT (OperationEnum operation,
			Integer value, 
			String key,
			String serverId,
			String userId)           {
		this.operation = operation;
		this.value       = value;
		this.key = key;
		this.serverId = serverId;
		this.userId = userId;
	}
	
	public Integer getValue() {
		return this.value;
	}
	public String getKey() {
		return this.key;
	} 
	public OperationEnum getOperation() {
		return this.operation;
	}
	public String getServerId() {
	return this.serverId;
	}
	public String getUserId() {
		return this.userId;
	}
	/*
	// PUT_MAP
	public OperationsDHT (OperationEnum operation,
			DHT_Map map, 
			boolean isReplica)           {
		this.operation = operation;
		this.map       = map;
		this.isReplica = isReplica;
	}

	// GET_MAP REMOVE_MAP CONTAINS_KEY_MAP
	public OperationsDHT (OperationEnum operation,
			String key,           
			boolean isReplica) {
		this.operation = operation;
		this.key       = key;
		this.isReplica = isReplica;
	}

	// KEY_SET_HM, VALUES_HM, INIT	
	public OperationsDHT (OperationEnum operation) {
		this.operation = operation;
	}

	//RETURN_VALUE
	public OperationsDHT (OperationEnum operation,
			Integer value)           {
		this.operation = operation;
		this.value     = value;
	}

	//RETURN_STATUS
	public OperationsDHT (OperationEnum operation,
			boolean status)           {
		this.operation  = operation;
		this.status     = status;
	}

	//DATA_REPLICA
	public OperationsDHT ( OperationEnum operation, 
			DHTUserInterface dht, int posReplica, int posServer) {
		this.operation   = operation;
		this.dht         = dht;
		this.posReplica  = posReplica;
		this.posServer   = posServer;
	}

	//DHT_REPLICA
	public OperationsDHT ( OperationEnum operation, 
			HashMap<Integer, Address> DHTServers) {
		this.operation   = operation;
		this.DHTServers  = DHTServers;
	}
	
	public OperationEnum getOperation() {
		return operation;
	}

	public void setOperation(OperationEnum operation) {
		this.operation = operation;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public DHT_Map getMap() {
		return map;
	}

	public void setMap(DHT_Map map) {
		this.map = map;
	}

	public boolean getStatus() {
		return status;
	}

	public void setMap(boolean status) {
		this.status = status;
	}

	public boolean isReplica() {
		return isReplica;
	}

	public void setReplica(boolean isReplica) {
		this.isReplica = isReplica;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public DHTUserInterface getDHT() {
		return this.dht;
	}
	
	public int getPosReplica() {
		return this.posReplica;
	}
	
	public HashMap<Integer, Address> getDHTServers() {
		return this.DHTServers;
	}

	*/
}

