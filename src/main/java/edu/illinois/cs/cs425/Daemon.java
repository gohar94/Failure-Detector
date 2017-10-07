package edu.illinois.cs.cs425;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.cli.GnuParser; 
import org.apache.commons.cli.CommandLine; 
import org.apache.commons.cli.CommandLineParser; 
import org.apache.commons.cli.HelpFormatter; 
import org.apache.commons.cli.Option; 
import org.apache.commons.cli.Options; 
import org.apache.commons.cli.ParseException; 
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.concurrent.atomic.AtomicReference;
import java.text.SimpleDateFormat;


/**
 * This class is responsible for glueing together all modules.  
 */
public class Daemon {
    private final static Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public static CopyOnWriteArrayList<String> members = new CopyOnWriteArrayList<String>();
    public static DatagramSocket client;
    public static boolean leader;
    public static String timeStamp;
    public static String leaderAddress;
    public static int leaderPort;
    public static InetAddress localhostName;
    public static int pingerPort ;
    public static int listenerPort;
    public static int daemonPort_temp;
    public static String path = "members.ser";
    public static boolean simulate = false; // Should packet drops in Network be simulated?
    public static int rate = 10; // Rate of Network packet drops

    public static void init() {
        try {
            //client = new DatagramSocket(daemonPort_temp);
            leader = false;
            localhostName = InetAddress.getLocalHost();
            pingerPort = 6666;
            listenerPort = 6667;
            daemonPort_temp = 6665;

            logger.info("Default ports are: ");
            logger.info("   daemonPort_temp:"+Integer.toString(daemonPort_temp));
            logger.info("   pingerPort: "+Integer.toString(pingerPort));
            logger.info("   listenerPort: "+Integer.toString(listenerPort));
    
            System.out.println(localhostName.toString());  // tobe delete
            //add log;
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }

    /**
     * This method is the entry point of the system which keeps all modules synchronized.
     */
    public static void main(String[] args) {
        try {
            Random rand = new Random();
            logger.setLevel(Level.SEVERE);
            logger.info("Daemon started");

            init();

            // get user options
            StringBuffer sb = new StringBuffer();
            for (int i = 0;i < args.length; i++) {
                sb.append(args[i]);
                sb.append(" ");
            }
            String s = sb.toString();
            timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());

            final Options options = new Options();
            final CommandLineParser parser = new GnuParser();

            Option l = new Option("l","leader",false,"declare leader");
            options.addOption(l);

            Option r = new Option("r","recover",false,"recover members list");
            options.addOption(r);

            Option a = new Option("a","address",true,"address of leader");
            options.addOption(a);

            Option p = new Option("p","port",true,"daemon client port");
            options.addOption(p);
            
            final CommandLine commandLine = parser.parse(options,args);

            if (commandLine.hasOption("l")) {
                logger.info("Option l is found!");
                logger.info("Leader declared at: "+localhostName.getHostName());
                
                leader = true;
            }

            if (commandLine.hasOption("a")) {
                //TODO send join message to host
                logger.info("Option a is found!");

                String[] leaderInfo = commandLine.getOptionValue("a").split(":");
                leaderAddress = leaderInfo[0];
                leaderPort = Integer.parseInt(leaderInfo[1]);

                logger.info("Current leader is "+leaderAddress+":"+leaderPort); 

            }    
            
            if (commandLine.hasOption("p")) {
                logger.info("Option p is found!");
                listenerPort = Integer.parseInt(commandLine.getOptionValue("p"));
                pingerPort = listenerPort-1;
                daemonPort_temp = listenerPort-2;

                logger.info("Port specified: "+Integer.toString(daemonPort_temp)+"\n"+
                    "Set daemonPort at "+Integer.toString(daemonPort_temp)+"\n"+
                    "Set listenerPort at "+Integer.toString(listenerPort)+"\n"+
                    "Set pingerPort at "+Integer.toString(pingerPort)+"\n");
            }

            if (commandLine.hasOption("r")) {
                logger.info("Option r is found!");
                readFromBackup();
		client = new DatagramSocket(daemonPort_temp);
                // TODO Send the new member list a broadcast that the leader is alive again - a join message
                // With the new ID this time
                String newleader = localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp;
                broadCast("join",newleader);
		logger.info("Broad cast new leader!");
            } 

            logger.info("Daemon start a listener: "+localhostName.getHostName()+":"+Integer.toString(listenerPort));
            Listener listener = new Listener(listenerPort,members,leader,timeStamp);
            listener.start();
            
        
            logger.info("Daemon start a pinger: "+localhostName.getHostName()+":"+Integer.toString(pingerPort));
            Pinger pinger = new Pinger(members, pingerPort);
            pinger.start();

            Backup backup = new Backup(members, path);

            if (leader == false) {
                //initial the memberlist by sending the first "join" message to leader's listener. leader will send back a current memberlist to local listener	
		client = new DatagramSocket(daemonPort_temp);

                logger.info("Start an UDP client for initialize!");

                InetAddress leaderIP = InetAddress.getByName(leaderAddress);
                JSONObject jsonArgument = new JSONObject();
                String message = localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp;
                jsonArgument.put("join", message);
                byte[] sendData = jsonArgument.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData,sendData.length,leaderIP,leaderPort);
                client.send(sendPacket);

                logger.info("request member list from leader "+leaderAddress.toString()+Integer.toString(leaderPort));

                //client.close();     //TODO should close when leave      

                //logger.info("Close client");
            } else {
		if(client == null)
		{
			client = new DatagramSocket(daemonPort_temp);
                }
		backup.start();
            }

            // UserHandler
            logger.info("User Handler starting!");
            
            Scanner scanner = new Scanner(System.in);
            String message = "";
            
            while (true) {
                message = scanner.next();
                if (message.equals("l") || message.equals("leave")) {
                    logger.info("Leave command issued by user.");

                    // TODO Broadcast message to all members here, 3 times
                    JSONObject jsonBroadcast = new JSONObject();
                    String myID = localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp;
                    jsonBroadcast.put("leave",myID);
                    for (int i = 0; i < 3; i++) {
                        logger.info("Broadcast round: "+Integer.toString(i));
                        Iterator<String> it = members.iterator();        
                        while (it.hasNext()) {      
                            String memberInfo = it.next();
                            byte[] sendBroadcast = jsonBroadcast.toString().getBytes();
                            InetAddress memberIP = InetAddress.getByName(memberInfo.split("//")[0]);
                            String memberPort = memberInfo.split("//")[1];
                            DatagramPacket broadcastPacket = new DatagramPacket(sendBroadcast,sendBroadcast.length,memberIP,Integer.parseInt(memberPort));
                            
                            if (Daemon.simulate) {
                                int randNum = rand.nextInt(100);
                                if (randNum <= Daemon.rate) {
                                    logger.severe("Dropping packet");
                                } else {
                                    client.send(broadcastPacket); // Ping is sent
                                }
                            } else {
                                client.send(broadcastPacket); // Ping is sent
                            }

                            logger.info("Leave message broadcast to "+memberInfo);
                        }
                        //Sleep 100ms before start next broadcast round
                        Thread.sleep(100);  
                    }
                    if (members.size() == 0) {
                        logger.info("Empty member list - quitting!");
                    }
                    pinger.stopMe();
                    listener.stopMe();
                    if (leader == true) {
                        backup.stopMe();
                    }
                    client.close();
                    logger.info("Quitting - All threads stopped!");
                    return;                    
                } else if (message.equals("p") || message.equals("print")) {
                    printMemberlist();
                } else if (message.equals("i") || message.equals("id")) {
                    logger.info("This process has ID:");
                    System.out.println("*******************************");
                    System.out.println(localhostName.getHostName()+"//"+Integer.toString(listenerPort)+"//"+timeStamp);
                    System.out.println("*******************************");
                } else if (message.equals("t") || message.equals("toggle")) {
                    // This is for toggling the value of "simulate" variable for simulating packet drops
                    logger.info("Toggling the value of simulate");
                    simulate = !simulate;
                    logger.info("New value of simulate is " + simulate);
                } else {
                    logger.info("Unknown command. Try again.");
                }
            }

            //pinger.join(); // is it correct?? don't know
            //listener.join();
        } catch (Exception e) {
            logger.info(e.toString());
        }
    }

    /** 
    * This function is to print current member list to screen
    */
    public static void printMemberlist() {
        logger.info("Current member list is:");
        Iterator<String> it = members.iterator();
        System.out.println("*******************************");
        int i=0;
        while (it.hasNext()) {
              String member = it.next();
              System.out.println("*"+member);
              i++;
        }
        System.out.println("*Total members: "+ Integer.toString(i));
        System.out.println("*******************************");
    }

    /**
     * This function is to broadcast to all member in the list
     */
    public static void broadCast(String keyWord,String content) {
        try {
            Iterator<String> it = members.iterator();
            JSONObject jsonBroadcast = new JSONObject();
            jsonBroadcast.put(keyWord,content);
            while (it.hasNext()) {      
                String member = it.next();
                byte[] sendBroadcast = jsonBroadcast.toString().getBytes();
                InetAddress memberIP = InetAddress.getByName(member.split("//")[0]);
                String memberPort = member.split("//")[1];
                DatagramPacket broadcastPacket = new DatagramPacket(sendBroadcast,sendBroadcast.length,memberIP,Integer.parseInt(memberPort));
                client.send(broadcastPacket);
            }
        } catch(Exception e) {
            logger.severe(e.toString());
        }
    }
    /** 
     * This function is to read the members list from backup if starting a node
     */
    public static void readFromBackup() {
        logger.info("Recovering from backup file " + path);
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            members = (CopyOnWriteArrayList<String>) in.readObject();
            in.close();
            fileIn.close();
            logger.info("Recovered members list successfully!");
        } catch(IOException i) {
            logger.severe(i.toString());
            return;
        } catch(ClassNotFoundException c) {
            logger.severe(c.toString());
            return;
        }
    }
}
