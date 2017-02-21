import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Thread.*;


/**
 * Created by utsavdholakia on 1/29/17.
 * Node is the main class that is executed at the startup and reads the config file to build knowledge
 * of its neighbours
 */

public class Node{
    public static String configFileName = "./Topology-Discovery/launch/config.txt";
    //Integer = Node ID, ArrayList<String> = 0 -> Hostname, 1 -> Port number
    public static volatile ConcurrentHashMap<Integer, ArrayList<String>> neighbours =
            new ConcurrentHashMap<Integer, ArrayList<String>>();
    public static Integer nodeID;       //Current Node's nodeID
    public static String currHostName;  //Current Node's hostname
    public static Integer currPortNum;  //Current Node's port number
    public static Integer totalNodes;   //Total nodes in topology
    public static volatile Integer roundOfNode = 1;  //Indicates node is in which round
    public static volatile ConcurrentHashMap<Integer, ArrayList<Integer>> topologyKnowledge =
            new ConcurrentHashMap<Integer, ArrayList<Integer>>(); //Knowledge gathered about topology by current node,
    // Integer = Node ID, ArrayList<Integer> = 0 -> Hop-count, 1 -> Done marked by this node(0 or 1)(only when all its children did)?
    public static volatile boolean localTermination = false;//Stop sending original messages, keep receiving and replying
    public static volatile boolean globalTermination = false;//Stop all events and server and the whole program
    public static TreeMap<Integer, HashSet<Integer>> myKnowledge = new TreeMap<Integer, HashSet<Integer>>();

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
                else if(!splitArray[1].startsWith("dc")){
                    validLines--;
                    //Found current Node's neighbour list
                    String [] neighbourID = sCurrentLine.split(delimiters);
                    BufferedReader scanForAddress;
                    ArrayList<Integer> listForNode = new ArrayList<Integer>();
                    Receiver.toplogyKnowledge.put(Integer.valueOf(neighbourID[0]), listForNode);
                    if(Node.nodeID == Integer.valueOf(neighbourID[0])) {
                        listForNode = new ArrayList<Integer>(2);
                        listForNode.add(0);
                        listForNode.add(0);
                        Node.topologyKnowledge.put(Node.nodeID, listForNode); //Add current Node's ID with hop-count 0
                    }
                    for(int i = 1; i < neighbourID.length; i++){
                        if(Node.nodeID == Integer.valueOf(neighbourID[0])) {
                            listForNode = new ArrayList<Integer>(2);
                            listForNode.add(1);
                            listForNode.add(0);
                            //Add all current neighbours with 1-hop distance in the topologyKnowledge map
                            Node.topologyKnowledge.put(Integer.valueOf(neighbourID[i]), listForNode);
                        }
                        Receiver.toplogyKnowledge.get(Integer.valueOf(neighbourID[0])).add(Integer.valueOf(neighbourID[i]));
                        //Mark done as false from the immediate neighbour(Its branch has not been discovered completely)
                        //Node.doneMarkedByNeighbour.put(Integer.valueOf(neighbourID[i]), false);
                        scanForAddress = new BufferedReader(new FileReader(configFileName));
                        //Read hostname and port number for every neighbour ID using new reader from the start
                        String line, hostName;
                        Integer portNum;
                        while((line = scanForAddress.readLine()) != null && Node.nodeID == Integer.valueOf(neighbourID[0])){
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
            }
            Sender.process();
            //Set the roundOfNode = 1
            Node.roundOfNode = 1;
            //Spawn the thread of the server/receiver class
            final Thread server = new Thread(new Receiver(Node.currPortNum, Node.currHostName),"Server");
            server.start();

            //Make the thread sleep for 5 seconds until the servers are up
            sleep(3000);
            //Spawn the thread of sender class that will send out messages to all neighbours
            Thread client = new Thread(new Sender(Node.neighbours, Node.topologyKnowledge), "Sender");
            client.start();

            Timer timer = new Timer();
            TimerTask tasknew = new TimerTask() {
                @Override
                public void run() {
                    System.out.println("Global termination marked, server has stopped.");
                    Node.globalTermination = true;
                    Receiver.messagesToBeProcessed.clear();
                    Processor.threadRunning = false;
                    Receiver.repliesExpectedFromNeighbours.clear();
                    Receiver.futureRoundMessageQueue.clear();
                    Sender.printOutput();
                }
            };
            timer.schedule(tasknew, 10000);
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
                    //ex.printStackTrace();
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
    public boolean termination = false;    //If the node sending this message has said it has locally terminated
    Message(Integer srcNodeID, Integer round, HashMap<Integer, ArrayList<Integer>> nodesDiscovered,
            Stack<Integer> parentList, Integer hopCount, boolean reply, boolean done){
        this.srcNodeID = srcNodeID;
        this.round = round;
        this.nodesDiscovered = nodesDiscovered;
        this.parentList = parentList;
        this.hopCount = round;  //Initialize hopcount with the current round of the node
        this.reply = reply;
        this.termination = termination;
    }
}

class Sender implements Runnable{
    Socket clientSocket;
    ConcurrentHashMap<Integer, ArrayList<String>> neighbours;
    ConcurrentHashMap<Integer, ArrayList<Integer>> topologyKnowledge;
    Message message;
    /*//Map for storing How many messages with a given source node ID are propagated for by current Node
    public static volatile HashMap<Integer, Integer> messagesSentForSomeNode = new HashMap<Integer, Integer>();*/
    Sender(ConcurrentHashMap<Integer, ArrayList<String>> neighbours, ConcurrentHashMap<Integer, ArrayList<Integer>> topologyKnowledge){
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
                    //System.out.println(e);
                }
            }
            /*else{
                System.out.println("Neighbour : " + neighbourID + " has marked itself done : " +
                        Node.topologyKnowledge.get(neighbourID).get(1));
            }*/
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

    public static void process(){
        Integer hopcount = 0;
        Receiver.explored.add(Node.nodeID);
        Receiver.process.add(Node.nodeID);
        while(!Receiver.process.isEmpty()) {
            int size = Receiver.process.size(), i = 0;
            HashSet<Integer> toBeAdded = new HashSet<Integer>();
            while(i < size){
                Integer popped = Receiver.process.poll();
                toBeAdded.add(popped);
                ArrayList<Integer> list = Receiver.toplogyKnowledge.get(popped);
                for(Integer node : list){
                    if(!Receiver.explored.contains(node)){
                        Receiver.explored.add(node);
                        Receiver.process.add(node);
                    }
                }
                i++;
            }
            if(hopcount != 0){
                Node.myKnowledge.put(hopcount, toBeAdded);
            }
            hopcount++;
        }
    }

    public static void printOutput(){

        System.out.println("Node_ID ---------------- HOP_COUNT ---------------- NEIGHBOURS");
        System.out.print(Node.nodeID);
        Iterator<Integer> iterateOverHops = Node.myKnowledge.keySet().iterator();
        HashSet<Integer> nodes;
        Integer hopCount;
        while(iterateOverHops.hasNext()){
            hopCount = iterateOverHops.next();
            System.out.print("                      " + hopCount + "                          ");
            nodes = Node.myKnowledge.get(hopCount);
            for(Integer node : nodes){
                System.out.print(node + ", ");
            }
            System.out.println();
        }
        System.out.println("----------------------------------------------------------------");
    }
}

class Receiver implements Runnable{
    Integer currPortNum;
    String currHostName;
    //This structure stores all the messages that are going to be processed
    public static volatile ConcurrentLinkedQueue<Message> messagesToBeProcessed = new ConcurrentLinkedQueue<Message>();
    //This structure allows us to buffer messages till we reach the round of the message
    public static Queue<Message> futureRoundMessageQueue = new LinkedList<Message>();
    //This structure will store all the messages originated at a particular node has been propagated and how many replies
    // are expected from its children; <Int = Node ID, Int = No of messages sent, that many replies expected>
    public static HashMap<Integer, ArrayList<Integer>> toplogyKnowledge = new HashMap<Integer, ArrayList<Integer>>();
    public static HashSet<Integer> explored = new HashSet<Integer>();
    public static Queue<Integer> process = new LinkedList<Integer>();
    public static volatile HashMap<Integer, Integer> repliesExpectedFromNeighbours = new HashMap<Integer, Integer>();
    //This structure will store all the messages originated at a particular node has been propagated and the replies are
    // expected from its children, till it comes back; <Int = Node ID, LinkedList<Message> = Message storage>
    public static volatile HashMap<Integer, LinkedList<Message>> bufferReplyMessages =
            new HashMap<Integer, LinkedList<Message>>();

    Receiver(Integer currPortNum, String currHostName) {
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
            while (!Node.globalTermination) {
                //If global termination is marked, break the loop and shut down the server
                if(Node.globalTermination){
                    serverSocket.close();
                    break;
                }
                Socket sock = serverSocket.accept();
                //System.out.println("Connected");
                new Thread(new ServerHandler(sock), "ServerHandler").start();
                if(Receiver.messagesToBeProcessed.size() > 0 && !Processor.threadRunning){
                    System.out.println("There are messages in the process queue, but processor thread is not running, so start it");
                    new Thread(new ServerHandler(sock), "Processor").start();
                }
            }
            //Process and print output as the server is also terminated
        } catch(Exception e){
            System.out.println("Could not create server on port number : " + currPortNum );
            e.printStackTrace();
        }
    }
}

class ServerHandler implements Runnable{
    public static Socket myClientSocket;

    ServerHandler(Socket socket){
        this.myClientSocket = socket;
    }

    @Override
    public void run(){
        try {
            if (Node.globalTermination) {
                System.out.println("Server has already stopped");
                return;
            }
            //Read the new Message that came on the server and add it to the list of messages to be processed
            Receiver.messagesToBeProcessed.add((Message)new ObjectInputStream(myClientSocket.getInputStream()).readObject());
            System.out.println("Accepted Client Address - " + myClientSocket.getInetAddress().getHostName());
            //If the processor class is not running, then start its thread so it starts processing
            if(!Processor.threadRunning){
                Processor.threadRunning = true;
                new Thread(new Processor(myClientSocket)).start();
                System.out.println("Started processor thread...");
            }

        }catch(EOFException eof){
            //Do nothing
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                myClientSocket.close();
            } catch (IOException ioe) {
                //ioe.printStackTrace();
            }
        }
    }
}
class Processor implements Runnable {
    public static volatile Boolean threadRunning = false;
    Socket myClientSocket;
    Boolean increasedRound = false;

    Processor(Socket s) {
        myClientSocket = s;
    }

    public void run() {
            //Iterate over all the messages that need to be processed
            for(Message inMessage : Receiver.messagesToBeProcessed){
                //Remove the first message and start processing it
                Receiver.messagesToBeProcessed.remove();
                System.out.println("InMessage : src node ID : " + inMessage.srcNodeID + " round : " + inMessage.round);
                //If the current round of node less than the round of message
                if(inMessage.round > Node.roundOfNode){
                    //Add the message to the queue which will process it in the future
                    Receiver.futureRoundMessageQueue.add(inMessage);
                    continue;
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
                                }
                                else {
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
                            increasedRound = true;
                            System.out.println("Increased node's round to : " + Node.roundOfNode);
                            if(!Node.localTermination && increasedRound) {
                                increasedRound = false;
                                new Thread(new Sender(Node.neighbours, Node.topologyKnowledge), "Sender").start();
                            }
                            if(!Receiver.futureRoundMessageQueue.isEmpty()){
                                if(Node.roundOfNode == Receiver.futureRoundMessageQueue.peek().round){
                                    for(Message message : Receiver.futureRoundMessageQueue){
                                        if(Node.roundOfNode == Receiver.futureRoundMessageQueue.peek().round) {
                                            Receiver.messagesToBeProcessed.add(message);
                                            System.out.println("Message added to the process message list");
                                            Receiver.futureRoundMessageQueue.remove();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    //The messages is not a reply, which is not a valid message, discard it
                    else {
                        //Do nothing, go to finally and close the socket and the thread
                        System.out.println("Not a valid case, reply came for me without reply flag set");
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
                                    neighboursDiscovered = false;
                                }
                            }
                            //All the neighbours are already discovered, so mark myself as done
                            if (neighboursDiscovered) {
                                neighbour = inMessage.nodesDiscovered.get(Node.nodeID);
                                neighbour.add(1, 1);//Mark myself done
                                inMessage.nodesDiscovered.put(Node.nodeID, neighbour);
                            }
                            sendMessageToParent(inMessage);
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
                                //Now mark done flag for myself if all my neighbours are marked done, and send consolidated
                                // message to parent
                                Iterator<Integer> it = Node.neighbours.keySet().iterator();
                                Boolean markMyselfDone = true;
                                while(it.hasNext()){
                                    if(inMessage.nodesDiscovered.get(it.next()).get(1) == 0){
                                        markMyselfDone = false;
                                    }
                                }
                                if(markMyselfDone){
                                    inMessage.nodesDiscovered.get(Node.nodeID).set(1, 1);
                                }
                                else{
                                    inMessage.nodesDiscovered.get(Node.nodeID).set(1, 0);
                                }
                                sendMessageToParent(inMessage);
                            }
                            //If all the replies are not yet back, then buffer this reply
                            else {
                                LinkedList<Message> repliesYet = Receiver.bufferReplyMessages.get(inMessage.srcNodeID);
                                repliesYet.add(inMessage);
                                Receiver.bufferReplyMessages.put(inMessage.srcNodeID, repliesYet);
                            }
                        }
                    }
                    //Hop-count is not zero, so message has to be propagated ahead
                    else {
                        //Propagate the incoming message to your neighbours which are not already in the knowledge
                        Integer neighbourID;
                        Iterator<Integer> it = Node.neighbours.keySet().iterator();
                        while(it.hasNext()) {
                            neighbourID = it.next();
                            //The neighbour that we want to send message to is already not available in knowledge
                            if(!inMessage.nodesDiscovered.containsKey(neighbourID)){
                                try {
                                    //Store how many messages are sent for current Node by the current Node
                                    if(!Receiver.repliesExpectedFromNeighbours.containsKey(inMessage.srcNodeID)){
                                        Receiver.repliesExpectedFromNeighbours.put(inMessage.srcNodeID, 1);
                                    }
                                    else{
                                        Integer value = Receiver.repliesExpectedFromNeighbours.get(inMessage.srcNodeID);
                                        Receiver.repliesExpectedFromNeighbours.put(inMessage.srcNodeID, value + 1);
                                    }
                                    Integer currHopCount = inMessage.nodesDiscovered.get(Node.nodeID).get(0);
                                    ArrayList<Integer> nodeInfo = new ArrayList<Integer>(2);
                                    nodeInfo.add(currHopCount+1);
                                    nodeInfo.add(0);
                                    inMessage.nodesDiscovered.put(neighbourID, nodeInfo);
                                    inMessage.parentList.push(Node.nodeID);
                                    ArrayList<String> hostPort = Node.neighbours.get(neighbourID);
                                    System.out.println("Propagating message to :" + hostPort.get(0));
                                    Socket clientSocket = new Socket(hostPort.get(0), Integer.valueOf(hostPort.get(1)));
                                    ObjectOutputStream outMessage = new ObjectOutputStream(clientSocket.getOutputStream());
                                    outMessage.writeObject(inMessage);
                                    clientSocket.close();
                                } catch (Exception e) {
                                    //System.out.println(e);
                                }
                            }
                            else{
                                System.out.println("Neighbour : " + neighbourID + " is already in the knowledge : ");
                            }
                        }
                    }
                }
            }

            //Mark my thread as terminated, if any new message comes in, new thread will be started...
            threadRunning = false;
    }

    public void sendMessageToParent(Message inMessage){
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
                //System.out.println(e);
            }
        } else {
            System.out.println("ERROR : Parent list(Stack) is empty...");
            System.out.println("Wanted to send below message");
            System.out.println("OutMessage : src node ID : " + inMessage.srcNodeID + " round : " + inMessage.round);
        }
    }
}