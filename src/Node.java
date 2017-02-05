import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

import static java.util.Collections.*;

/**
 * Created by utsavdholakia on 1/29/17.
 * Node is the main class that is executed at the startup and reads the config file to build knowledge
 * of its neighbours
 */

public class Node{
    public static String configFileName = "./out/production/Topology-Discovery/Configuration.txt";
    //Integer = Distance in hops, ArrayList<String> = 0 -> Hostname, 1 -> Port number
    public static volatile HashMap<Integer, ArrayList<String>> neighbours = new HashMap<Integer, ArrayList<String>>();
    public static Integer nodeID;       //Current Node's nodeID
    public static String currHostName;  //Current Node's hostname
    public static Integer currPortNum;  //Current Node's port number
    public static Integer totalNodes;   //Total nodes in topology
    public static volatile Integer roundOfNode;  //Indicates node is in which round
    public static volatile Integer noOfMessagesSent; //Indicates no. of outbound messages in this round
    public static volatile HashMap<Integer, ArrayList<Integer>> topologyKnowledge =
            new HashMap<Integer, ArrayList<Integer>>(); //Knowledge gathered about topology by current node
    public static volatile boolean localTermination = false;//Stop sending original messages, keep receiving and replying
    public static volatile boolean globalTermination = false;//Stop all events and server and the whole program

    Node(){}

    public static void main(String []args){
        /*if(args.length <= 0){
            System.out.println("Provide nodeID");
            System.exit(0);
        }
        nodeID  = Integer.valueOf(args[0]);*/
        //Read the configuration file to build knowledge of neighbours
        BufferedReader br = null;
        FileReader fr = null;
        try {

            fr = new FileReader(configFileName);
            br = new BufferedReader(fr);

            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                System.out.println(sCurrentLine);
                String delimiters = "\\s+";
                String[] splitArray;
                if(sCurrentLine.length() <= 1 || sCurrentLine.charAt(0) == '#') {
                    //If line is empty
                    continue;
                }
                else if(sCurrentLine.indexOf('#') != -1) {
                    //Take into account only lines without comments and/or portions before comment starts
                    sCurrentLine = sCurrentLine.substring(0, sCurrentLine.indexOf('#'));
                    splitArray = sCurrentLine.split(delimiters);
                }
                else {
                    splitArray = sCurrentLine.split(delimiters);
                }
                if(splitArray[1].equals(InetAddress.getLocalHost().getHostName())){
                    //If the current node's hostname is matched with the line that contains it
                    Node.nodeID = Integer.valueOf(splitArray[0]);
                    Node.currHostName = splitArray[1];
                    Node.currPortNum = Integer.valueOf(splitArray[2]);
                }
                else if(!splitArray[1].startsWith("dc") && (Integer.valueOf(splitArray[0]) == Node.nodeID)){
                    //Found current Node's neighbour list
                    String [] neighbourID = sCurrentLine.split(delimiters);
                    BufferedReader scanForAddress;
                    for(int i = 1; i < neighbourID.length; i++){
                        scanForAddress = new BufferedReader(new FileReader(configFileName));
                        //Read hostname and port number for every neighbour ID using new reader from the start
                        String line, hostName;
                        Integer portNum;
                        while((line = scanForAddress.readLine()) != null){
                            if(line.length() <= 1){ //When it's total nodes number
                                continue;
                            }
                            String[] splitLine = line.split(delimiters);
                            /*if(!splitLine[1].startsWith("dc")){   //TODO Remove comments and enable this block
                                continue;
                            }*/
                            if(Integer.valueOf(splitLine[0]) != Integer.valueOf(neighbourID[i])){
                                //If the node which is not a neighbour
                                continue;
                            }
                            else if(Integer.valueOf(splitLine[0]) == Integer.valueOf(neighbourID[i])){
                                //Read neighbour node's hostname and port number and store it
                                if(splitLine.length >= 3){
                                    hostName = splitLine[1];
                                    portNum = Integer.valueOf(splitLine[2]);
                                    if(!Node.neighbours.containsKey(Integer.valueOf(splitLine[0]))){
                                        ArrayList<String> hostPort = new ArrayList<String>();
                                        hostPort.add(0, hostName);
                                        hostPort.add(1, portNum.toString());
                                        Node.neighbours.put(Integer.valueOf(splitLine[0]), hostPort);
                                    }
                                }
                                break;
                            }
                        }
                        scanForAddress.close();
                    }

                }
                else{
                        //Found some other Node's neighbour list
                        continue;
                }
            }

            //Set the roundOfNode = 1
            Node.roundOfNode = 1;

            //Spawn the thread of the server/receiver class
            Thread server = new Thread(new Receiver(Node.neighbours, Node.currPortNum, Node.currHostName),"Server");
            server.start();

            //Make the thread sleep for 5 seconds until the servers are up
            Thread.sleep(5000);
            //Spawn the thread of sender class that will send out messages to all neighbours
            Thread client = new Thread(new Sender(Node.neighbours), "Sender");
            client.start();


        } catch (FileNotFoundException e1) {
                e1.printStackTrace();
            } catch (IOException e1) {
                e1.printStackTrace();
            } catch(Exception e){
                e.printStackTrace();
            } finally {
                try {
                    if (br != null)
                        br.close();
                    if (fr != null)
                        fr.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
    }

}

class Message implements Serializable{
    //Structure of the messages that are created by each node to communicate
    public Integer srcNodeID;  //Source of the message
    public Integer round;  //Which round is on the source node
    public HashMap<Integer, ArrayList<Integer>> nodesDiscovered; //<Hop count from the source node, List of those nodes>
    public Stack<Integer> parentList;  //Parents in a reverse order through which path the done message will travel
    public Integer hopCount;    //Total no. of hops that is allowed for this message to traverse
    public boolean reply = false;   //If this message is a reply back to the source node or not
    public boolean done = false;    //If the node that the message has reached to, has nowhere to send it ahead
    Message(Integer srcNodeID, Integer round, HashMap<Integer, ArrayList<Integer>> nodesDiscovered,
            Stack<Integer> parentList, Integer hopCount, boolean reply, boolean done){
        this.srcNodeID = srcNodeID;
        this.round = round;
        this.nodesDiscovered = nodesDiscovered;
        this.parentList = parentList;
        this.hopCount = round;  //Initialize hopcount with the current round of the node
        this.reply = reply;
        this.done = done;
    }
}

class Sender implements Runnable{
    Socket clientSocket;
    HashMap<Integer, ArrayList<String>> neighbours;
    Message message;
    Set<Integer> keySet;
    Sender(HashMap<Integer, ArrayList<String>> neighbours){
        this.neighbours = neighbours;
    }

    @Override
    public void run() {
        composeMessage();
        Iterator<Integer> it = keySet.iterator();
        while(it.hasNext()) {
            try {
                Node.noOfMessagesSent++;    //Increase number of messages sent by 1
                ArrayList<String> hostPort = neighbours.get(it.next());
                System.out.println("Sending message to :" + hostPort.get(0));
                clientSocket = new Socket(hostPort.get(0), Integer.valueOf(hostPort.get(1)));
                ObjectOutputStream outMessage = new ObjectOutputStream(clientSocket.getOutputStream());
                outMessage.writeObject(message);
                clientSocket.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    public void composeMessage(){
        keySet = neighbours.keySet();
        ArrayList<Integer> nodes = new ArrayList<Integer>();
        HashMap<Integer, ArrayList<Integer>> nodesDiscovered = new HashMap<Integer, ArrayList<Integer>>();
        Stack<Integer> parents = new Stack<Integer>();
        //Add each neighbour currently connected at 1-hop distance into an arraylist
        for(Integer key : keySet){
            nodes.add(key);
        }
        Set<Integer> hopCount = nodesDiscovered.keySet();
        if(hopCount != null && hopCount.size() != 0){
            Integer maxHopCount = Collections.max(hopCount);    //Find maximum hopcount so increment to it and then add
            nodesDiscovered.put(maxHopCount++, nodes);          //into nodesDiscoverd with that hopcount as a key
        }else{
            nodesDiscovered.put(1, nodes);                   //No nodes yet, so add 1-hop neighbours
        }
        parents.push(Node.nodeID);                              //Add self into the stack of parents
        //When the source of this node generates the message, the hopcount = round of the node, reply = false,
        // done = false
        message = new Message(Node.nodeID, Node.roundOfNode, nodesDiscovered, parents, Node.roundOfNode,
                false, false);
    }
}

class Receiver implements Runnable{
    static Boolean serverOn = true;
    Integer currPortNum;
    HashMap<Integer, ArrayList<String>> neighbours;
    String currHostName;

    Receiver(HashMap<Integer, ArrayList<String>> neighbours, Integer currPortNum,
             String currHostName) {
        this.neighbours = neighbours;
        this.currPortNum = currPortNum;
        this.currHostName = currHostName;
    }

    @Override
    public void run(){
        ServerSocket serverSocket;
        try{
            //Initialize the receiver as a continuous listening server
            serverSocket = new ServerSocket(currPortNum);
            System.out.println("Listening");
            while (serverOn) {
                Socket sock = serverSocket.accept();
                System.out.println("Connected");
                new Thread(new Processor(sock), "Processor").start();
            }
        } catch(Exception e){
            System.out.println("Could not create server on port number : " + currPortNum );
            e.printStackTrace();
        }
    }
}

class Processor implements Runnable {
    Socket myClientSocket;
    public static volatile Queue<Message> queueForMessage = new LinkedList<Message>();

    Processor(Socket s) {
        myClientSocket = s;
    }

    public void run() {
        System.out.println("Accepted Client Address - " + myClientSocket.getInetAddress().getHostName());
        try {
            if (!Receiver.serverOn) {
                System.out.print("Server has already stopped");
            }
            Message inMessage = (Message)new ObjectInputStream(myClientSocket.getInputStream()).readObject();
            System.out.println("SRC NODE ID : "+ inMessage.srcNodeID);
            System.out.println("ROUND : " + inMessage.round);
            /*int index = 0;
            while(!inMessage.parentList.isEmpty()){
                System.out.println("PARENT(" + index + ") : " + inMessage.parentList.pop());
            }
            Iterator<Integer> it = inMessage.nodesDiscovered.keySet().iterator();
            int hopcount = 0;
            while(it.hasNext()){
                hopcount = it.next();
                System.out.println("Hop Count : " +hopcount + ", NodeID : " + inMessage.nodesDiscovered.get(hopcount));
            }*/
            //If the current round of node is not equal to the round of message
            while(inMessage.round != Node.roundOfNode){
                //Add the message to the queue of future messages
                if(!queueForMessage.contains(inMessage)){
                    queueForMessage.add(inMessage);
                }
                //Make the thread sleep for 3 seconds
                Thread.sleep(3000);
            }
            //If the message came with source node ID the same as current node's ID
            if(inMessage.srcNodeID == Node.nodeID){
                if(inMessage.reply == true) {
                    Node.noOfMessagesSent--;
                    //Decrease the number of messages sent and if 0 then increase round of current node
                    if (Node.noOfMessagesSent == 0) {
                        Node.roundOfNode++;
                    }
                    if(!Node.localTermination){
                        new Thread(new Sender(Node.neighbours), "Sender");
                    }
                    else{
                        //Local termination is marked, discard message and close connection
                    }
                }
                //The messages is not a reply, which is not a valid message, discard it
                else{
                    //Do nothing, go to finally and close the socket and the thread
                }
            }
            //If the message came with source node ID other than the current node's ID
            else if(inMessage.srcNodeID != Node.nodeID){

            }

        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                myClientSocket.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }
}