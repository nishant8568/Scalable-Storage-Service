package client;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.TextMessage;
/**
 * KVStore which performs the functions of the Storage server 
 * @param It takes address and port of the server
 * 
 */
public class KVStore implements KVCommInterface {

	private static Logger logger = Logger.getRootLogger();
	private int port;
	private ServerSocket serverSocket;
	private Socket clientSocket;
	private boolean running;
	private String address;
	private static volatile HashMap<String, String> h = null;
	private String message = "";
	private KVMessage kvMsg;
	final static int MAX_SIZE_KEY=20;
	final static int MAX_SIZE_VALUE=1024*120;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.port = port;
		this.address = address;
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Inside KVStore connect=" + clientSocket);
		message = new String("Connection to KV server established: "
				+ clientSocket.getLocalAddress() + " / "
				+ clientSocket.getLocalPort());
		System.out.println("Inside KVStore connect" + message);
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		String message = new String("Connection to KV server disconnected: "
				+ clientSocket.getLocalAddress() + " / "
				+ clientSocket.getLocalPort());
		this.message = message;

	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		TextMessage tempTextMsg;
		KVMessage.StatusType tempStatus = null;
		HashMap<String, String> hashMap = getHashMap();
		String transac = KVMessage.StatusType.PUT.toString(), result = "";
		// Invalid key check
		if(key.getBytes().length>MAX_SIZE_KEY)
		{
			result = KVMessage.StatusType.PUT_ERROR.toString();
			logger.error("KVStore | put | operation put failed as the key length is more than 20 bytes |key="+key);
			tempTextMsg = new TextMessage("type:put,statuscode:" + result + ",key:" + key
					+ ",value:" + value + ",comments:Key length is more than 20B ");
			tempTextMsg.setKey(key);
			tempTextMsg.setStatus(KVMessage.StatusType.PUT_ERROR);
			tempTextMsg.setValue(value);
			tempTextMsg.setComments("Invalid_Key");
			kvMsg = tempTextMsg;
			return kvMsg;
		}
		
		//Invalid key value
		if(value.getBytes().length>MAX_SIZE_VALUE)
		{
			result = KVMessage.StatusType.PUT_ERROR.toString();
			logger.error("KVStore | put | operation put failed as the value length is more than 120 KB | value="+value);
			tempTextMsg = new TextMessage("type:put,statuscode:" + result + ",key:" + key
					+ ",value:" + value + ",comments:Value length is more than 20B ");
			tempTextMsg.setKey(key);
			tempTextMsg.setStatus(KVMessage.StatusType.PUT_ERROR);
			tempTextMsg.setValue(value);
			tempTextMsg.setComments("Invalid_Value");
			kvMsg = tempTextMsg;
			return kvMsg;
		}
		
		// Deletion of tuple for keys whose values are null
		if ("null".equals(value))
		{
			try {
				if(!hashMap.containsKey(key))
				{
					logger.info("***********Key="+key+" does not exist");	
					tempStatus=KVMessage.StatusType.DELETE_ERROR;
					tempTextMsg = new TextMessage("type:put,statuscode:" + KVMessage.StatusType.DELETE_ERROR.toString() + ",key:" + key
							+ ",value:" + value + ",comments:KEY_ABSENT");
					tempTextMsg.setKey(key);				
					tempTextMsg.setValue(value);
					tempTextMsg.setStatus(KVMessage.StatusType.DELETE_ERROR);
					tempTextMsg.setComments("KEY_ABSENT");
					kvMsg = tempTextMsg;
					return kvMsg;
				}
				hashMap.remove(key);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("***********Key="+key+" could not be removed from the Server"+e);	
				tempStatus=KVMessage.StatusType.DELETE_ERROR;
				tempTextMsg = new TextMessage("type:put,statuscode:" + KVMessage.StatusType.DELETE_ERROR.toString() + ",key:" + key
						+ ",value:" + value + ",comments:Exception occured while deletion ");
				tempTextMsg.setKey(key);				
				tempTextMsg.setValue(value);
				tempTextMsg.setStatus(KVMessage.StatusType.DELETE_ERROR);
				tempTextMsg.setComments("Exception occured while deletion ");
				kvMsg = tempTextMsg;
				return kvMsg;
			}
				logger.info("***********Key="+key+" is removed from the Server");	
				tempStatus=KVMessage.StatusType.DELETE_SUCCESS;
				tempTextMsg = new TextMessage("type:put,statuscode:" + KVMessage.StatusType.DELETE_SUCCESS + ",key:" + key
						+ ",value:" + value + ",comments:Deletion Successful");
				tempTextMsg.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
				tempTextMsg.setComments("Deletion Successful");
				kvMsg = tempTextMsg;
				return kvMsg;
		}
		
		
		String valueExist = (String) hashMap.get(key);
		if (valueExist != null && !"".equals(key)) {
			hashMap.remove(key);
			result = KVMessage.StatusType.PUT_UPDATE.toString();
			tempStatus=KVMessage.StatusType.PUT_UPDATE;
		} else {
			logger.info("**************************Its a new key =" + key
					+ " old value=" + valueExist);		
		}
		try {
			hashMap.put(key, value);
			if ("".equals(result)) {
				result = KVMessage.StatusType.PUT_SUCCESS.toString();
				tempStatus=KVMessage.StatusType.PUT_SUCCESS;
			}
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("KVStore | put | operation put failed" + e);
			result = KVMessage.StatusType.PUT_ERROR.toString();
			tempStatus=KVMessage.StatusType.PUT_ERROR;
		}
		tempTextMsg = new TextMessage("type:put,statuscode:" + result + ",key:" + key
				+ ",value:" + value + ",comments:" + transac);
		logger.info("type:put,statuscode:" + result + ",key:" + key + ",value:"
				+ value + ",comments:" + transac);
		tempTextMsg.setKey(key);
		tempTextMsg.setStatus(tempStatus);
		tempTextMsg.setValue(value);
		tempTextMsg.setComments("transac");
		kvMsg = tempTextMsg;
		return kvMsg;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		String value="", result="";
		TextMessage tempTextMsg;
		KVMessage.StatusType tempStatus = null;
		try {
			System.out.println("Inside get section");
			HashMap<String, String> hashMap = getHashMap();
			// System.out.println("get request key="+key);
			System.out.println("get request keylength=" + key.length());
			key = key.trim().replace("\n", "").replace("\r", "");
			value = (String) hashMap.get(key);
			if ("".equals(value) || null == value) {
				result = KVMessage.StatusType.GET_ERROR.toString();
				tempStatus = KVMessage.StatusType.GET_ERROR;
			} else {
				value = value.trim();
				result = KVMessage.StatusType.GET_SUCCESS.toString();
				tempStatus = KVMessage.StatusType.GET_SUCCESS;
			}

			logger.info("type:get,statuscode:" + result + ",key:" + key
					+ ",value:" + value + ",comments:" + result);
			logger.info("get hashmap contains key"
					+ hashMap.containsKey((Object) key));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in KVStore | method: get " + e);
			result = KVMessage.StatusType.GET_ERROR.toString();
		}
		tempTextMsg = new TextMessage("type:get,statuscode:" + result + ",key:" + key
				+ ",value:" + value + ",comments:" +  KVMessage.StatusType.GET.toString());
		tempTextMsg.setComments(KVMessage.StatusType.GET.toString());
		tempTextMsg.setKey(key);
		tempTextMsg.setStatus(tempStatus);
		tempTextMsg.setValue(value);
		kvMsg=tempTextMsg;
		return kvMsg;
	}

	public KVMessage send(String value) throws Exception {
		kvMsg = new TextMessage(value);
		return kvMsg;
	}

	public ServerSocket getServerSocket() {
		return serverSocket;
	}

	public void setServerSocket(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}

	static synchronized HashMap<String, String> getHashMap() {
		if (h == null) {
			h = new HashMap<String, String>();
		}
		return h;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Socket getClientSocket() {
		return clientSocket;
	}

	public void setClientSocket(Socket clientSocket) {
		this.clientSocket = clientSocket;
	}

}
