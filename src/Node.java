import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.*;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by utsavdholakia on 1/29/17.
 * Node is the main class that is executed at the startup and reads the config file to build knowledge
 * of its neighbours
 */

public class Node{
    public static String configFileName = "./launch/config.txt";
    //Integer = Node ID, ArrayList<String> = 0 -> Hostname, 1 -> Port number
    public static volatile HashMap<Integer, ArrayList<String>> neighbours = new HashMap<Integer, ArrayList<String>>();
    public static Integer nodeID;       //Current Node's nodeID
    public static String currHostName;  //Current Node's hostname
    public static Integer currPortNum;  //Current Node's port number
    public static Integer totalNodes;   //Total nodes in topology
    public static volatile Integer roundOfNode = 1;  //Indicates node is in which round
    public static volatile Integer noOfMessagesSent = 0; //Indicates no. of outbound messages in this round
    public static volatile HashMap<Integer, ArrayList<Integer>> topologyKnowledge =
            new HashMap<Integer, ArrayList<Integer>>(); //Knowledge gathered about topology by current node,
    // Integer = Node ID, ArrayList<Integer> = 0 -> Hop-count, 1 -> Done marked by this node(0 or 1)(only when all its children did)?
    public static volatile boolean localTermination = false;//Stop sending original messages, keep receiving and replying
    public static volatile boolean globalTermination = false;//Stop all events and server and the whole program
    public static volatile HashMap<Integer, Integer> repliesExpected = new HashMap <Integer, Integer>();//Mark
    // how many neighbours we have sent messages to(Originated by some node), so we should expect replies from them
    // before moving the message upwards <Integer = Originator Node ID, Integer = How many messages forwarded by me>
    /*public static volatile HashMap<Integer, Integer> neighbourDoneMarkedForANode = new HashMap<Integer, Integer>();//This
    // structure saves which messages(originated from which nodes) have got how many done flags from its children*/

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
            Integer validLines = 0;
            while ((sCurrentLine = br.readLine()) != null) {
                System.out.println(sCurrentLine);
                ArrayList<Integer> listForNode;
                String delimiters = "\\s+[\t]*|\\s*[\t]+";
                String[] splitArray;
                sCurrentLine = sCurrentLine.trim();
                if(sCurrentLine.length() < 1 || sCurrentLine.charAt(0) == '#') {
                    //If line is empty or starts with a comment
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
                //Read the total Nodes value from the file
                if(splitArray.length == 1){
                    Node.totalNodes = Integer.valueOf(splitArray[0]);
                    validLines = Node.totalNodes * 2;   //Total number of valid lines in config file
                    continue;
                }
                if(splitArray[1].equals(InetAddress.getLocalHost().getHostName())){
                    //If the current node's hostname is matched with the line that contains it
                    Node.nodeID = Integer.valueOf(splitArray[0]);
                    Node.currHostName = splitArray[1];
                    Node.currPortNum = Integer.valueOf(splitArray[2]);
                    validLines--;
                }
                else if(!splitArray[1].startsWith("dc") && (Integer.valueOf(splitArray[0]) == Node.nodeID)){
                    //Found current Node's neighbour list
                    String [] neighbourID = sCurrentLine.split(delimiters);
                    BufferedReader scanForAddress;
                    listForNode = new ArrayList<Integer>(2);
                    listForNode.add(0);
                    listForNode.add(0);
                    Node.topologyKnowledge.put(Node.nodeID, listForNode); //Add current Node's ID with hop-count 0
                    for(int i = 1; i < neighbourID.length; i++){
                        listForNode = new ArrayList<Integer>(2);
                        listForNode.add(1);
                        listForNode.add(0);
                        //Add all current neighbours with 1-hop distance in the topologyKnowledge map
                        Node.topologyKnowledge.put(Integer.valueOf(neighbourID[i]), listForNode);
                        //Mark done as false from the immediate neighbour(Its branch has not been discovered completely)
                        //Node.doneMarkedByNeighbour.put(Integer.valueOf(neighbourID[i]), false);
                        scanForAddress = new BufferedReader(new FileReader(configFileName));
                        //Read hostname and port number for every neighbour ID using new reader from the start
                        String line, hostName;
                        Integer portNum;
                        while((line = scanForAddress.readLine()) != null){
                            String[] splitLine;
                            if(line.length() < 1 || line.charAt(0) == '#') {
                                //If line is empty or starts with a comment
                                continue;
                            }
                            else if(line.indexOf('#') != -1) {
                                //Take into account only lines without comments and/or portions before comment starts
                                line = line.substring(0, sCurrentLine.indexOf('#'));
                                splitLine = line.split(delimiters);
                            }
                            else {
                                splitLine = line.split(delimiters);
                            }
                            /*if(!splitLine[1].startsWith("dc")){   //TODO Remove comments and enable this block
                                continue;
                            }*/
                            if(splitLine.length > 1){
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
            Thread client = new Thread(new Sender(Node.neighbours, Node.topologyKnowledge), "Sender");
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
    public HashMap<Integer, ArrayList<Integer>> nodesDiscovered; //<Node, (0 -> Hop-count, 1 -> Done marked by this node?)>
    public Stack<Integer> parentList;  //Parents in a reverse order through which path the done message will travel
    public Integer hopCount;    //Total no. of hops that is allowed for this message to traverse
    public boolean reply = false;   //If this message is a reply back to the source node or not
    //public boolean done = false;    //If the node that the message has reached to, has nowhere to send it ahead
    Message(Integer srcNodeID, Integer round, HashMap<Integer, ArrayList<Integer>> nodesDiscovered,
            Stack<Integer> parentList, Integer hopCount, boolean reply, boolean done){
        this.srcNodeID = srcNodeID;
        this.round = round;
        this.nodesDiscovered = nodesDiscovered;
        this.parentList = parentList;
        this.hopCount = round;  //Initialize hopcount with the current round of the node
        this.reply = reply;
        //this.done = done;
    }
}

class Sender implements Runnable{
    Socket clientSocket;
    HashMap<Integer, ArrayList<String>> neighbours;
    HashMap<Integer, ArrayList<Integer>> topologyKnowledge;
    Message message;
    /*//Map for storing How many messages with a given source node ID are propagated for by current Node
    public static volatile HashMap<Integer, Integer> messagesSentForSomeNode = new HashMap<Integer, Integer>();*/
    Sender(HashMap<Integer, ArrayList<String>> neighbours, HashMap<Integer, ArrayList<Integer>> topologyKnowledge){
        this.neighbours = neighbours;
        this.topologyKnowledge = topologyKnowledge;
    }

    @Override
    public void run() {
        composeMessage();
        Integer neighbourID;
        Iterator<Integer> it = neighbours.keySet().iterator();
        while(it.hasNext()) {
            neighbourID = it.next();
            //The neighbour that we want to send message to has already not marked itself as done
            if(Node.topologyKnowledge.get(neighbourID).get(1) == 0){
                try {
                    Node.noOfMessagesSent++;    //Increase number of messages sent by 1
                    ArrayList<String> hostPort = neighbours.get(neighbourID);
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
    }

    public void composeMessage(){
        Set<Integer> keySet = topologyKnowledge.keySet();   //Find out nodeID of all the neighbours
        HashMap<Integer, ArrayList<Integer>> nodesDiscovered = new HashMap<Integer, ArrayList<Integer>>();
        Stack<Integer> parents = new Stack<Integer>();
        //Add all the nodes with their hop-count and their done status from the knowledge
        for(Integer key : keySet){
            nodesDiscovered.put(key, topologyKnowledge.get(key));   //Add all the nodes with their hop-count from the knowledge
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
    String currHostName;
    public static volatile Queue<Message> queueForMessage = new LinkedList<Message>();

    Receiver(HashMap<Integer, ArrayList<String>> neighbours, Integer currPortNum,
             String currHostName) {
        this.currPortNum = currPortNum;
        this.currHostName = currHostName;
    }

    @Override
    public void run(){
        ServerSocket serverSocket;
        try{
            //Initialize the receiver as a continuous listening server
            serverSocket = new ServerSocket(currPortNum);
            System.out.println("Listening on port : " + currPortNum);
            while (serverOn) {
                //If global termination is marked, break the loop and shut down the server
                if(Node.globalTermination){
                    serverOn = false;
                    break;
                }
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
    public static ArrayList<Message> messagesToBeProcessed = new ArrayList<Message>();;

    Processor(Socket s) {
        myClientSocket = s;
    }

    public void run() {
        System.out.println("Accepted Client Address - " + myClientSocket.getInetAddress().getHostName());
        try {
            if (!Receiver.serverOn) {
                System.out.println("Server has already stopped");
                return;
            }
            //Read the new Message that came on the server and add it to the list of messages to be processed
            messagesToBeProcessed.add((Message)new ObjectInputStream(myClientSocket.getInputStream()).readObject());
            for(Message inMessage : messagesToBeProcessed) {
                System.out.println("InMessage : src node ID : " + inMessage.srcNodeID + " round : " + inMessage.round);
                //If the current round of node less than the round of message
                while (inMessage.round > Node.roundOfNode) {
                    //Add the message to the queue of future messages
                    if (!Receiver.queueForMessage.contains(inMessage)) {
                        System.out.println("InMessage round > Node round, adding it to queue");
                        Receiver.queueForMessage.add(inMessage);
                    }
                    //Make the thread sleep for 3 seconds
                    Thread.sleep(3000);
                }
                //If the message came with source node ID the same as current node's ID
                if (inMessage.srcNodeID.equals(Node.nodeID)) {
                    if (inMessage.reply == true) {
                        Iterator<Integer> it = inMessage.nodesDiscovered.keySet().iterator();
                        Integer key = 0;
                        while (it.hasNext()) {
                            key = it.next();
                            //An info of an already existing node in the knowledge
                            if (Node.topologyKnowledge.containsKey(key)) {
                                //If the hop-count in the current topology knowledge for this node(key) is greater than the
                                // one we got in the message
                                if (Node.topologyKnowledge.get(key).get(0) > inMessage.nodesDiscovered.get(key).get(0)) {
                                    Node.topologyKnowledge.put(key, inMessage.nodesDiscovered.get(key));
                                } else {
                                    //Do nothing as the values got in the reply is of a higher hop-count than we already have
                                }
                            }
                            //Add an info of a new node with its hop-count
                            else {
                                Node.topologyKnowledge.put(key, inMessage.nodesDiscovered.get(key));
                            }
                        }
                        Node.noOfMessagesSent--;
                        //If the message came from node and all the messages have come

                        if ((Node.noOfMessagesSent == 0)) {
                            Node.localTermination = true;
                        }
                        //Decrease the number of messages sent and if 0 then increase round of current node
                        if (Node.noOfMessagesSent == 0) {
                            if (!Node.localTermination) {
                                Node.roundOfNode++;
                                System.out.println("Increased node's round to : " + Node.roundOfNode);
                                if(Receiver.queueForMessage.size() > 0){
                                    Iterator<Message> processMessageIterator = Receiver.queueForMessage.iterator();
                                    while(processMessageIterator.hasNext()){
                                        Message nextMessage = processMessageIterator.next();
                                        //If the message in a queue has the same round as the node's round, then add it to process list
                                        if(nextMessage.round == Node.roundOfNode){
                                            messagesToBeProcessed.add(Receiver.queueForMessage.remove());
                                        }
                                    }
                                }
                                else{
                                    new Thread(new Sender(Node.neighbours, Node.topologyKnowledge), "Sender").start();
                                }
                            } else {
                                System.out.println("Local termination marked. Node will not send any original messages now!");
                                //Local termination is marked, discard message and close connection
                            }
                        } else {
                            //Reply message came from a neighbour, round is not to be increased
                            // because some messages have yet not arrived
                        }
                    }
                    //The messages is not a reply, which is not a valid message, discard it
                    else {
                        //Do nothing, go to finally and close the socket and the thread
                    }
                }
                //If the message came with source node ID other than the current node's ID
                else if (!inMessage.srcNodeID.equals(Node.nodeID)) {
                    inMessage.hopCount--;   //To decide whether to propagate this message further or not

                    if (inMessage.hopCount == 0) {    //Don't forward this message, just reply back
                        inMessage.reply = true; //Set reply flag true
                        Integer dstParent = inMessage.parentList.pop();
                        ArrayList<String> destination = Node.neighbours.get(dstParent);
                        try {
                            System.out.println("Sending message to :" + destination.get(0));
                            System.out.println("OutMessage : src node ID : " + inMessage.srcNodeID + " round : " + inMessage.round);
                            Socket clientSocket = new Socket(destination.get(0), Integer.valueOf(destination.get(1)));
                            ObjectOutputStream outMessage = new ObjectOutputStream(clientSocket.getOutputStream());
                            outMessage.writeObject(inMessage);
                            clientSocket.close();
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                    } else {

                    }
                }
            }
        }catch(EOFException eof){
          //Do nothing
        } catch (Exception e) {
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