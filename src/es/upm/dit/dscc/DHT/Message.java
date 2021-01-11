package es.upm.dit.dscc.DHT;

import java.io.Serializable;
public class Message implements Serializable {

	private static final long serialVersionUID = 1L;
	private OperationEnum operation;
	private Integer     value         = null;       
	private String      key           = null;
	//private String 		serverId;
	//private DHT_Map     map           = null;
	//private boolean     status        = false;
	//private boolean     isReplica     = false;
	//private int         posReplica;
	//private int         posServer;
	//private DHTUserInterface dht      = null;
	// private HashMap<Integer, Address> DHTServers;
	// private Set<String> 
	// private ArrayList<Integer>
	
	public Message (OperationEnum operation,
			Integer value, 
			String key)           {
		this.operation = operation;
		this.value       = value;
		this.key = key;
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
	
}
