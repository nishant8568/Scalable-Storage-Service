package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvClient.KVClient;


public class KVServer  implements Runnable{

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 */


	private static Logger logger = Logger.getRootLogger();
	private int port;
	private boolean running;
	private ServerSocket serverSocket;

	public KVServer(int port) {
		this.port=port;

	}
	@Override
	public void run() {
		// TODO Auto-generated method stub

		running = initializeServer();

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();                
					KVClient connection = 
							new KVClient(client,serverSocket);
					new Thread(connection).start();

					logger.info("Connected to " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");

	}

	private boolean isRunning() {
		return this.running;
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public void stopServer(){
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}
	/**
	 * Starts the server in the particular port. 
	 * 
	 */
	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());    
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}
	/**
	 * Main method which starts the server 
	 * @param It takes an optional command line argument of the log level 
	 * Default log level is INFO
	 */
	public static void main(String[] args) {
		
		try {
			if(args.length==0)				
				new LogSetup("logs/server.log", Level.INFO);
			else{
				if ("trace".equalsIgnoreCase(args[0]))
					new LogSetup("logs/server.log", Level.TRACE);
				else if ("debug".equalsIgnoreCase(args[0]))
					new LogSetup("logs/server.log", Level.DEBUG);
				else if ("info".equalsIgnoreCase(args[0]))
					new LogSetup("logs/server.log", Level.INFO);
				else if ("fatal".equalsIgnoreCase(args[0]))
					new LogSetup("logs/server.log", Level.FATAL);
				else if ("error".equalsIgnoreCase(args[0]))
					new LogSetup("logs/server.log", Level.ERROR);
				else if ("all".equalsIgnoreCase(args[0]))
					new LogSetup("logs/server.log", Level.ALL);
				else if ("off".equalsIgnoreCase(args[0]))
					new LogSetup("logs/server.log", Level.OFF);
				else
				{
					logger.error("Enter valid log levels ....-"+LogSetup.getPossibleLogLevels());
				}
			}

			Thread t = new Thread(new KVServer(10000), "Server");
			t.start();
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("Exception in KVServer --------"+e);
			e.printStackTrace();
		}
	}
}
