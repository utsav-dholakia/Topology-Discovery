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
            //The neighbour that we want to send message to is already not available in knowledge
            if(Node.topologyKnowledge.get(neighbourID).get(1) != 1){
                try {
                    //Store how many messages are sent for current Node by the current Node
                    if(!Receiver.repliesExpectedFromNeighbours.containsKey(Node.nodeID)){
                        Receiver.repliesExpectedFromNeighbours.put(Node.nodeID, 1);
                    }
                    else{
                        Integer value = Receiver.repliesExpectedFromNeighbours.get(Node.nodeID);
                        Receiver.repliesExpectedFromNeighbours.put(Node.nodeID, value + 1);
                    }
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
            else{
                System.out.println("Neighbour : " + neighbourID + " has marked itself done : " +
                        Node.topologyKnowledge.get(neighbourID).get(1));
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
    //This structure stores all the messages that are going to be processed
    public static ArrayList<Message> messagesToBeProcessed = new ArrayList<Message>();
    //This structure allows us to buffer messages till we reach the round of the message
    public static volatile Queue<Message> queueForMessage = new LinkedList<Message>();
    //This structure will store all the messages originated at a particular node has been propagated and how many replies
    // are expected from its children; <Int = Node ID, Int = No of messages sent, that many replies expected>
    public static volatile HashMap<Integer, Integer> repliesExpectedFromNeighbours = new HashMap<Integer, Integer>();
    //This structure will store all the messages originated at a particular node has been propagated and the replies are
    // expected from its children, till it comes back; <Int = Node ID, LinkedList<Message> = Message storage>
    public static volatile HashMap<Integer, LinkedList<Message>> bufferReplyMessages =
            new HashMap<Integer, LinkedList<Message>>();

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
            Receiver.messagesToBeProcessed.add((Message)new ObjectInputStream(myClientSocket.getInputStream()).readObject());
            for(Message inMessage : Receiver.messagesToBeProcessed) {
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
                        Integer value = Receiver.repliesExpectedFromNeighbours.get(Node.nodeID);
                        Receiver.repliesExpectedFromNeighbours.put(Node.nodeID, value - 1);
                        //If all the neighbours have replied back
                        //Decrease the number of messages got replied back and if 0 then increase round of current node
                        if (Receiver.repliesExpectedFromNeighbours.get(Node.nodeID) == 0) {
                            //Check if localTermination is to be marked
                            Boolean doneMarkedByAllNeighbours = true;
                            Iterator<Integer> iterateNeighbours = Node.neighbours.keySet().iterator();
                            while (it.hasNext()) {
                                //If a neighbour node has not marked itself done
                                if (Node.topologyKnowledge.get(it.next()).get(1) == 0) {
                                    doneMarkedByAllNeighbours = false;
                                    break;
                                }
                            }
                            //If all the neighbours marked themselves done
                            if (doneMarkedByAllNeighbours) {
                                Node.localTermination = true;
                                System.out.println("Local termination marked. Node will not send any original messages now!");
                            }
                            //Increase node's round as all neighbours replied back
                            Node.roundOfNode++;
                            System.out.println("Increased node's round to : " + Node.roundOfNode);
                            if (Receiver.queueForMessage.size() > 0) {
                                Iterator<Message> processMessageIterator = Receiver.queueForMessage.iterator();
                                while (processMessageIterator.hasNext()) {
                                    Message nextMessage = processMessageIterator.next();
                                    //If the message in a queue has the same round as the node's round, then add it to process list
                                    if (nextMessage.round == Node.roundOfNode) {
                                        Receiver.messagesToBeProcessed.add(Receiver.queueForMessage.remove());
                                        System.out.println("Message added to the process message list");
                                    }
                                }
                            }
                        }
                    }
                    //The messages is not a reply, which is not a valid message, discard it
                    else {
                        //Do nothing, go to finally and close the socket and the thread
                    }
                }
                //If the message came with source node ID other than the current node's ID
                else if (!inMessage.srcNodeID.equals(Node.nodeID)) {
                    if(inMessage.hopCount > 0) {
                        inMessage.hopCount--;   //To decide whether to propagate this message further or not
                    }

                    if (inMessage.hopCount == 0) {    //Don't forward this message, just reply back
                        //If the message is forwarded to me, but I'm the last hop for this round, so we'll mark reply true
                        if(!inMessage.reply) {
                            inMessage.reply = true;
                            //Integer myHopCount = inMessage.nodesDiscovered.get(Node.nodeID).get(0);//Check hopcount of myself
                            //Check if all my neighbours are in the knowledge or not
                            Iterator<Integer> iterateNeighbours = Node.neighbours.keySet().iterator();
                            Integer nextNeighbour;
                            Boolean neighboursDiscovered = true;
                            ArrayList<Integer> neighbour;
                            while (iterateNeighbours.hasNext()) {
                                nextNeighbour = iterateNeighbours.next();
                                //Neighbour found which is not in the discovered nodes list
                                if (!inMessage.nodesDiscovered.containsKey(nextNeighbour)) {
                                /*neighbour = new ArrayList<Integer>(2);
                                neighbour.add(nextNeighbour);
                                neighbour.add(0);
                                inMessage.nodesDiscovered.put(myHopCount+1, neighbour);*/
                                    neighboursDiscovered = false;
                                }
                            }
                            //All the neighbours are already discovered, so mark myself as done
                            if (neighboursDiscovered) {
                                neighbour = inMessage.nodesDiscovered.get(Node.nodeID);
                                neighbour.add(1, 1);//Mark myself done
                                inMessage.nodesDiscovered.put(Node.nodeID, neighbour);
                            }
                            if (!inMessage.parentList.isEmpty()) {
                                Integer dstParent = inMessage.parentList.pop();
                                ArrayList<String> destination = Node.neighbours.get(dstParent);
                                try {
                                    System.out.println("Sending message to : " + destination.get(0));
                                    System.out.println("OutMessage : src node ID : " + inMessage.srcNodeID + " round : " + inMessage.round);
                                    Socket clientSocket = new Socket(destination.get(0), Integer.valueOf(destination.get(1)));
                                    ObjectOutputStream outMessage = new ObjectOutputStream(clientSocket.getOutputStream());
                                    outMessage.writeObject(inMessage);
                                    clientSocket.close();
                                } catch (Exception e) {
                                    System.out.println(e);
                                }
                            } else {
                                System.out.println("ERROR : Parent list(Stack) is empty...");
                                System.out.println("Wanted to send below message");
                                System.out.println("OutMessage : src node ID : " + inMessage.srcNodeID + " round : " + inMessage.round);
                            }
                        }
                        //The message already has reply->true, so I have to forward it to my parent when I get all
                        // replies from my children for this messages's source node
                        else {
                            Integer value = Receiver.repliesExpectedFromNeighbours.get(inMessage.srcNodeID);
                            Receiver.repliesExpectedFromNeighbours.put(inMessage.srcNodeID, value - 1);
                            //If all the neighbours have replied back , consolidate all the reply messages stored for
                            // this source Node in the buffer and send it to our parent(check done condition)
                            if (Receiver.repliesExpectedFromNeighbours.get(inMessage.srcNodeID) == 0) {
                                Iterator<Message> messageIterator = Receiver.bufferReplyMessages.get(inMessage.srcNodeID).listIterator();
                                System.out.println("Consolidating below message for replying back...");
                                System.out.println("OutMessage : src node ID : " + inMessage.srcNodeID + " round : " + inMessage.round);
                                while(messageIterator.hasNext()){
                                    Message temp = messageIterator.next();
                                    Iterator<Integer> it = temp.nodesDiscovered.keySet().iterator();
                                    Integer key = 0;
                                    while (it.hasNext()) {
                                        key = it.next();
                                        //Try to mark done flag->on for any node by any message if it is there
                                        if((inMessage.nodesDiscovered.get(key).get(1) + temp.nodesDiscovered.get(key).get(1)) > 0){
                                            inMessage.nodesDiscovered.get(key).set(1, 1);
                                        }
                                        //Try to change the hop-count to whatever is minimal in all messages for each node
                                        if(inMessage.nodesDiscovered.containsKey(key)) {
                                            if (inMessage.nodesDiscovered.get(key).get(0) > temp.nodesDiscovered.get(key).get(0)) {
                                                inMessage.nodesDiscovered.put(key, temp.nodesDiscovered.get(key));
                                            }
                                        }
                                        //If any node is not present in the knowledge yet, add it
                                        else{
                                            inMessage.nodesDiscovered.put(key, temp.nodesDiscovered.get(key));
                                        }
                                    }
                                }
                            }
                            //If all the replies are not yet back, till then buffer this reply
                            else {
                                LinkedList<Message> repliesYet = Receiver.bufferReplyMessages.get(inMessage.srcNodeID);
                                repliesYet.add(inMessage);
                                Receiver.bufferReplyMessages.put(inMessage.srcNodeID, repliesYet);
                            }
                        }
                    }
                    //Hop-count is not zero, so message has to be propagated ahead
                    else {
                        Integer value = Receiver.repliesExpectedFromNeighbours.get(inMessage.srcNodeID);
                        Receiver.repliesExpectedFromNeighbours.put(inMessage.srcNodeID, value + 1);
                    }
                }
                if(!Node.localTermination) {
                    new Thread(new Sender(Node.neighbours, Node.topologyKnowledge), "Sender").start();
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