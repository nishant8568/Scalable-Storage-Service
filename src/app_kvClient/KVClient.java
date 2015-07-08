package app_kvClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

import client.KVStore;
import common.messages.TextMessage;

/**
 * Class which creates a thread for each client 
 * @param Client socket and Server socket
 */
public class KVClient implements Runnable{
	Socket clientSocket;
	ServerSocket serverSocket;
	private static Logger logger = Logger.getRootLogger();
	private KVStore kvCl;
	private boolean isOpen=true;
	private static final int BUFFER_SIZE = 1024;
	private InputStream input;
	private OutputStream output;


	public KVClient(Socket connection, ServerSocket serverSocket) {
		// TODO Auto-generated constructor stub
		this.clientSocket=connection;
		this.serverSocket=serverSocket;
	}

	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		logger.info("Inside KVClient - method run *********************");
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			kvCl = new KVStore(clientSocket.getInetAddress().getHostName(),clientSocket.getPort());
			kvCl.setServerSocket(serverSocket);
			kvCl.setClientSocket(clientSocket);
			kvCl.connect();
			String msg = kvCl.getMessage();
			logger.info("message="+msg);			
			sendMessage(new TextMessage(msg));

			while (isOpen) {
				try {
					receiveMessage();	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!" + ioe);
					isOpen = false;
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Error! Connection could not be established!", ex);

		} finally {

			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}

	}
	
	/**
	 * Receives the client message and unmarshalls the message to perform operation
	 * Loops until the connection is closed or aborted by the client.
	 */
	private TextMessage receiveMessage() throws IOException {

		TextMessage msg = null;

		try {
			int index = 0;			
			byte[] bufferBytes = new byte[BUFFER_SIZE];
			/* read first char from stream */
			byte read = (byte) input.read();
			/* carriage return */
			while (read != 13 ) { //Checking for a carriage return 
				try {
					bufferBytes[index] = read;
					index++;
					if(index==BUFFER_SIZE) // Recreating the buffer once it gets exhausted
					{
						bufferBytes=new byte[BUFFER_SIZE];
						index=0;
					}
					//	 read next char from stream 
					read = (byte) input.read();					
				} catch (Exception e) {
					// TODO Auto-generated catch block				
					logger.error("Exception while reading the input stream of the client |"+e);
				}
			}

			logger.info("The client message =" + new String(bufferBytes));
			String msgText = new String(bufferBytes);
			msgText=msgText.replace("\r", "").replace("\n", "").trim();
			String splitArr[] = msgText.split(",");
			String func = "";
			String key = "";
			String value = "";
			if(splitArr!=null){
				for (int i = 0; i < splitArr.length; i++) {
					if(logger.isDebugEnabled())
					{
						logger.debug(splitArr[i]);
					}					
					String element[] = splitArr[i].split(":");	
					if(logger.isDebugEnabled())
					{
						logger.debug("Individual element name1 : "+ element[0]);
					}
					
					if ("type".equals(element[0].trim())) {
						if ("get".equals(element[1].trim())) {
							func = "get";
						} else if ("put".equals(element[1].trim())) {
							func = "put";
						} else if ("send".equals(element[1].trim())) {
							func = "send";
						} else if ("disconnect".equals(element[1].trim())) {
							func = "disconnect";
						} else if ("get".equals(element[1].trim())) {
							func = "connect";
						}
					} else if ("key".equals(element[0].trim())) {
						key = element[1];
					} else if ("value".equals(element[0].trim())) {
						value = element[1];
					}

					if(logger.isDebugEnabled())
					{
						logger.debug("Individual element name2 : "+ element[1]);
					}								
				}
			}

			if ("get".equals(func)) {
				TextMessage kvMsg= (TextMessage)kvCl.get(key);
				sendMessage(kvMsg);

			} else if ("put".equals(func)) {					
				TextMessage kvMsg= (TextMessage)kvCl.put(key,value);
				sendMessage(kvMsg);
			}
			else if ("send".equals(func)) {					
				TextMessage kvMsg= (TextMessage)kvCl.send(value);
				sendMessage(kvMsg);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception in KVClient | receiveMessage() "+ e);
		}
		return msg;
	}
	/**
	 * Sends the final message and marshalls the result as per the client protocol
	 * 
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		String stringMsg=msg.getMsg()+"\r"+"\n";		
		byte[] msgBytes = stringMsg.getBytes();
		output.write(msgBytes);
		output.flush();		
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
	}
}
