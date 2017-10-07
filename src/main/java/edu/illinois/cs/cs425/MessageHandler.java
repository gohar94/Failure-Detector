package edu.illinois.cs.cs425;

import java.net.*;
import java.util.*;
import java.io.*;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is to handle the message received by listener
 */
public class MessageHandler extends Thread{
      private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	DatagramPacket receivePacket;
      DatagramSocket server;
      CopyOnWriteArrayList<String> members;
      boolean leader;
      String timestamp;
      int portnumber;
      private Random rand;

	public MessageHandler(DatagramSocket _server, DatagramPacket _receivePacket, CopyOnWriteArrayList<String> _members, boolean _leader, String _timestamp, int _portnumber) { 
            // TODO check that later
		server = _server;
            receivePacket = _receivePacket;
            members = _members;
            leader = _leader;
            timestamp = _timestamp;
            portnumber = _portnumber;
            rand = new Random();
	}

      /** 
       * This function is to print current member list to screen
       */
      public void printMemberlist()
      {
            logger.info("Current member list is:");
            Iterator<String> it = members.iterator();
            System.out.println("*******************************");
            int i=0;
            while(it.hasNext()){
                  String member = it.next();
                  System.out.println("*"+member);
                  i++;
            }
            System.out.println("*Total members: "+ Integer.toString(i));
            System.out.println("*******************************");
      }

      /**
       * This function is to broadcast message to all member in the group
      **/
      public void broadCast(String keyWord,String content)
      {
            try{
                  Iterator<String> it = members.iterator();
                  JSONObject jsonBroadcast = new JSONObject();
                  jsonBroadcast.put(keyWord,content);
                  while (it.hasNext()) {      
                        String member = it.next();
                        byte[] sendBroadcast = jsonBroadcast.toString().getBytes();
                        InetAddress memberIP = InetAddress.getByName(member.split("//")[0]);
                        String memberPort = member.split("//")[1];
                        DatagramPacket broadcastPacket = new DatagramPacket(sendBroadcast,sendBroadcast.length,memberIP,Integer.parseInt(memberPort));
                        
                        if (Daemon.simulate) {
                            int randNum = rand.nextInt(100);
                            if (randNum <= Daemon.rate) {
                                logger.severe("Dropping packet");
                            } else {
                                server.send(broadcastPacket); // Ping is sent
                            }
                        } else {
                            server.send(broadcastPacket); // Ping is sent
                        }

                        logger.info(keyWord+" message broadcast to "+member);
                  }
            }catch(Exception e)
            {
                  logger.info(e.toString());
            }
            
      }

      /** This function is to distinguish message type and take different reactions
       * Message type:
       * -join: leader send back the current memberlist, broadcast in the group and add the member to list; common member add member to list
       * -init: this perticular message is sent by leader only. follow by a array of member list.
       * -ping: send ack message back to the source
       * -pingreq: send ping message to target
       * -ack: this perticular message appears in indirect ping process. once receive the "ack" message, the process send another "ack" message to the source who start the pingreq.
       * -leave/-fail: remove the target from the member list 
      */
	public void run() {
		try {
			logger.info("MessageHandler started");

                  // Declare a new UDP packet for ack back 
                  DatagramPacket ackPacket;
                  JSONObject jsonAck = new JSONObject();
                  byte[] sendAck;
                  String ackMessage;
      

                  // Use JsonObject to handle the receiving message
                  String message = new String(receivePacket.getData());
                  JSONObject jsonArgument = new JSONObject(message);

                  if (jsonArgument.has("join")) { 
                        //If I am a leader, then broadcast , send the member list back | add new member to memberlist             	
                        String target = jsonArgument.getString("join");
                        logger.info("New join member"+target);
                        
                        if (leader) {
                              // Put all members in list to a JSONObject. Including leader itself

                              JSONArray memberlst_array = new JSONArray();
                              for (String member : members) {
                                    memberlst_array.put(member);
                              }
                              // Don't forget to add leaderID to the list 
                              String leaderID = InetAddress.getLocalHost().getHostName()+"//"+Integer.toString(portnumber)+"//"+timestamp;
                              memberlst_array.put(leaderID);
                              logger.info("add leaderID "+leaderID);
                              jsonAck.put("init",memberlst_array);
                              logger.info("Member list prepared!");

                              // Send memberlist back to the new join member
                              String memberlstInit = jsonAck.toString();
                              sendAck= memberlstInit.getBytes();
                              InetAddress targetIP = InetAddress.getByName(target.split("//")[0]);
                              String targetPort = target.split("//")[1];
                              ackPacket = new DatagramPacket(sendAck,sendAck.length,targetIP,Integer.parseInt(targetPort));
                              server.send(ackPacket); // Ping is sent
                              // if (Daemon.simulate) {
                              //     int randNum = rand.nextInt(100);
                              //     if (randNum <= Daemon.rate) {
                              //         logger.severe("Dropping packet");
                              //     } else {
                              //         server.send(ackPacket); // Ping is sent
                              //     }
                              // } else {
                              //     server.send(ackPacket); // Ping is sent
                              // }

                              logger.info("send initial memberlist to "+targetIP.toString()+":"+targetPort);
                              
                              // Broadcast the join message to all the members
                              for (int i = 0; i < 3; i++) {
                                    logger.info("Broadcast round: "+Integer.toString(i));
                                    broadCast("join",target);
                                    Thread.sleep(100);  
                              }

                              // Finally add the new join member to member list
                              members.add(target);
                              printMemberlist();
                        }
                        else {
                              //for common process(not the leader), check if the remoteID already in the memberlist,  
                              if (!members.contains(target)) {
                                    members.add(target);
                                    logger.info("add new member success!");
                                    printMemberlist();
                              }
                              else{
                                    logger.info("member already exist");
                              }
                              
                        }
                  } else if (jsonArgument.has("ping")) {
                  	logger.info(jsonArgument.getString("ping"));
                        String seqNum = jsonArgument.getString("ping");
                        String srcIp = jsonArgument.getString("srcIp");
                        String srcPort = jsonArgument.getString("srcPort");
                        String srcID = jsonArgument.getString("id");
                        logger.info("receive ping from "+srcIp+"//"+srcPort+"//"+srcID);
                        logger.info("seqNum of this ping is "+seqNum);

                        // Checking if this is a relay ping (For pingReq)
                        if (jsonArgument.has("isPingReq") && jsonArgument.has("returnPort") && jsonArgument.has("returnIp")) {
                              String returnIP = jsonArgument.getString("returnIp");
                              String returnPort = jsonArgument.getString("returnPort");
                              jsonAck.put("returnIp", returnIP);
                              jsonAck.put("returnPort", returnPort);
                              logger.severe("Got relay ping for " + srcID);
                        }

                        //Check if it is the right number
                        if (srcID.equals(timestamp)) {
                              logger.info("preparing ack for "+seqNum);
                              jsonAck.put("ack",seqNum);
                              ackMessage = jsonAck.toString();
                              sendAck = ackMessage.getBytes();
                              ackPacket = new DatagramPacket(sendAck,sendAck.length,InetAddress.getByName(srcIp),Integer.parseInt(srcPort));
                              
                              if (Daemon.simulate) {
                                  int randNum = rand.nextInt(100);
                                  if (randNum <= Daemon.rate) {
                                      logger.severe("Dropping packet");
                                  } else {
                                      server.send(ackPacket); // Ping is sent
                                  }
                              } else {
                                  server.send(ackPacket); // Ping is sent
                              }

                              logger.info("ack back sent for " + seqNum);
                              logger.info("ack packet size is " + sendAck.length);
                        } else {
                          logger.severe("Got a ping but for not my ID");
                        }
                  } else if (jsonArgument.has("init")) {
                        logger.info("receiving initial member list from leader!");
                  	JSONArray membership_array = jsonArgument.getJSONArray("init");
                  	
                        for (int i = 0; i < membership_array.length(); i++) {
                  		String member = membership_array.getString(i);
                  		members.add(member);
                              logger.info("Adding member " + member);
                  	}  

                        printMemberlist();  	
                  } else if (jsonArgument.has("pingReq")) {
                        logger.info("Got a pingReq message");
                        String seqNum = jsonArgument.getString("pingReq");
                        String target = jsonArgument.getString("target");
                        String targetIP = target.split("//")[0];
                        String targetPort = target.split("//")[1];
                        String targetID = jsonArgument.getString("id");
                        logger.info("ID got here is " + targetID);
                        String returnIP = jsonArgument.getString("srcIp");
                        String returnPort = jsonArgument.getString("srcPort");
                        
                        InetAddress IPAddress = InetAddress.getByName(targetIP);
                        byte[] sendData;

                        JSONObject pingObj = new JSONObject();
                        pingObj.put("ping", seqNum);
                        pingObj.put("id", targetID); // This is the unique ID of the node we are pinging
                        pingObj.put("srcIp", InetAddress.getLocalHost().getHostAddress());
                        pingObj.put("srcPort", Integer.toString(portnumber));
                        pingObj.put("isPingReq", "true");
                        pingObj.put("returnIp", returnIP);
                        pingObj.put("returnPort", returnPort);
                        
                        String ping = pingObj.toString();
                        sendData = ping.getBytes();

                        logger.info("Connecting to " + IPAddress.toString() + " via UDP port " + targetPort);
                        logger.info("Sending " + sendData.length + " bytes to server");
                        logger.info("Sending packet = " + ping);
                        
                        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(targetPort)); 
                        
                        if (Daemon.simulate) {
                            int randNum = rand.nextInt(100);
                            if (randNum <= Daemon.rate) {
                                logger.severe("Dropping packet");
                            } else {
                                server.send(sendPacket); // Ping is sent
                            }
                        } else {
                            server.send(sendPacket); // Ping is sent
                        }

                        logger.info("Sent " + sendData.length + " bytes to server");
                  } else if (jsonArgument.has("ack")) {
                        // Relaying back the ACK
                        String seqNum = jsonArgument.getString("ack");
                        String returnIP = jsonArgument.getString("returnIp");
                        String returnPort = jsonArgument.getString("returnPort");
                        
                        InetAddress IPAddress = InetAddress.getByName(returnIP);
                        byte[] sendData;

                        JSONObject pingObj = new JSONObject();
                        pingObj.put("ack", seqNum);
                        pingObj.put("srcIp", InetAddress.getLocalHost().getHostAddress());
                        pingObj.put("srcPort", Integer.toString(portnumber));
                        
                        String ping = pingObj.toString();
                        sendData = ping.getBytes();

                        logger.info("Connecting to " + IPAddress.toString() + " via UDP port " + returnPort);
                        logger.info("Sending " + sendData.length + " bytes to server");
                        logger.info("Sending packet = " + ping);
                        
                        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(returnPort)); 
                        
                        if (Daemon.simulate) {
                            int randNum = rand.nextInt(100);
                            if (randNum <= Daemon.rate) {
                                logger.severe("Dropping packet");
                            } else {
                                server.send(sendPacket); // Ping is sent
                            }
                        } else {
                            server.send(sendPacket); // Ping is sent
                        }

                        logger.info("Sent " + sendData.length + " bytes to server");
                  } else if (jsonArgument.has("leave") || jsonArgument.has("fail")) {
                        if (jsonArgument.has("leave")) {
                              logger.info(jsonArgument.getString("leave"));
                              String target = jsonArgument.getString("leave");
                              logger.info("Leave claim at member "+target);
                              if(members.contains(target)){
                                    boolean status = members.remove(target);
                                    logger.info("Remove member "+target+" status "+status);
                                    printMemberlist();
                              }
                              else{
                                    logger.info("Cannot remove "+ target+"\n"+"Member "+target+" already removed");
                              }
                              
                        } else if (jsonArgument.has("fail")) {
                              String target = jsonArgument.getString("fail");
                              String srcIp = jsonArgument.getString("srcIp");
                              String srcPort = jsonArgument.getString("srcPort");
                              logger.info("Receive failure detection message from " +srcIp+":"+srcPort);
                              if(members.contains(target))
                              {
                                    boolean status = members.remove(target);
                                    logger.info("Remove member "+target+" status "+status);
                                    printMemberlist();
                              }
                        }
                        //TODO delete from the memberlist;
                  }
		} catch(Exception e){
			logger.severe(e.toString());
		}
	}
}

