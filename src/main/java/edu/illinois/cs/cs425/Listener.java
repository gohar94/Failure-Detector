package edu.illinois.cs.cs425;

import java.util.*;
import java.io.*;
import java.net.*;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 *This class is to listening the message sent by other nodes
 */

public class Listener extends Thread {
	private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	DatagramSocket server;
	int portNumber;
	CopyOnWriteArrayList<String> members;
	boolean leader;
	String timestamp;
    boolean finished;

	public Listener(int _portNumber,CopyOnWriteArrayList<String> _members,boolean _leader, String _timestamp) {
		members = _members;
		portNumber = _portNumber;
		leader = _leader;
		timestamp = _timestamp;
        finished = false;
	}

    /**
     * This function is used to gracefully stop this thread if needed.
     */
    public void stopMe() {
        logger.info("Stopping");
        finished = true;
        logger.info("Stopped");
    }

	public void run(){	
		try {
			logger.info("Listener start");

			//initialize UDP server
			server = new DatagramSocket(portNumber);
            server.setSoTimeout(1000); // Just to go back and see if this thread is even alive or not, otherwise go back to waiting
            while (!Thread.currentThread().isInterrupted() && !finished) {
				//initialize UDP datagrampacket and waiting for data
				byte[] receiveData = new byte[1024];
				DatagramPacket receivePacket = new DatagramPacket(receiveData,receiveData.length);

				logger.info("waiting for data");

                try {
                    server.receive(receivePacket);
                } catch (Exception to) {
                    // This is most likely a timeout, keep waiting
                    continue;
                }
				//Thread block here unless receive a datagrampacket
				if (receivePacket != null) {
					//once receiving packet from a remote process, spraw a MessageHandler to handle the message
					logger.info("receive data from: "+receivePacket.getAddress()+":"+Integer.toString(receivePacket.getPort()));
					MessageHandler messagehandler = new MessageHandler(server,receivePacket,members,leader,timestamp,portNumber);
					//TODO leader,timestamp
				    messagehandler.start();
				}
			}
		    logger.info("Quitting Listener");	
		} catch (Exception e) {
			logger.info(e.toString());
		} finally {
			try{
				server.close();
			} catch (Exception e) {
				logger.info(e.toString());
			}
		}
	}
}
