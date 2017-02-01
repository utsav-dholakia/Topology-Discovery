import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Stack;
/**
 * Created by utsavdholakia on 1/29/17.
 * Below class is the main class that executed at the startup and reads the config file to build knowledge
 * of its neighbours
 */

public class Node{
    private static String configFileName = "./src/Configuration.txt";
    private HashMap<String, Integer> hostPort;
    private HashMap<Integer, HashMap<String, Integer>> neighbours = new HashMap<Integer, HashMap<String, Integer>>();
    public static Integer nodeID;  //Current Node's nodeID
    public static String currHostName;
    public static Integer currPortNum;
    public static Integer totalNodes;  //Total nodes in topology

    Node(){}

    public static void main(String []args){
        if(args.length <= 0){
            System.out.println("Provide nodeID");
            System.exit(0);
        }
        nodeID  = Integer.valueOf(args[0]);
        Node node = new Node();
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
                String[] splitArray = sCurrentLine.split(delimiters);
                if(sCurrentLine.length() <= 1) {
                    //If line is empty
                    continue;
                }
                else if(sCurrentLine.indexOf('#') != -1) {
                    //Take into account only lines without comments and/or portions before comment starts
                    sCurrentLine = sCurrentLine.substring(0, sCurrentLine.indexOf('#'));
                }
                else if(!splitArray[1].startsWith("dc") && (Integer.valueOf(splitArray[0]) == nodeID)){
                    //Found current Node's neighbour list
                    String [] neighbourID = sCurrentLine.split(delimiters);
                    for(int i = 1; i < neighbourID.length; i++){
                        //Read hostname and port number for every neighbour ID using new reader from the start
                        BufferedReader scanForAddress = new BufferedReader(new FileReader(configFileName));
                        String line, hostName;
                        Integer portNum;
                        while((line = scanForAddress.readLine()) != null){
                            if(line.length() <= 1){ //When it's total nodes number
                                continue;
                            }
                            String[] splitLine = line.split(delimiters);
                            if(!splitLine[1].startsWith("dc")){
                                continue;
                            }
                            if(Integer.valueOf(splitLine[0]) != Integer.valueOf(neighbourID[i])){
                                //If the node which is not a neighbour
                                continue;
                            }
                            else if(Integer.valueOf(splitLine[0]) == Integer.valueOf(splitArray[0])){
                                //If the value is of the current node
                                if(splitLine.length >= 3) {
                                    node.currHostName = splitLine[1];
                                    node.currPortNum = Integer.valueOf(splitLine[2]);
                                }
                            }
                            else if(Integer.valueOf(splitLine[0]) == Integer.valueOf(neighbourID[i])){
                                //Read neighbour node's hostname and port number and store it
                                if(splitLine.length >= 3){
                                    hostName = splitLine[1];
                                    portNum = Integer.valueOf(splitLine[2]);
                                    if(!node.neighbours.containsKey(Integer.valueOf(splitLine[0]))){
                                        node.hostPort = new HashMap<String, Integer>();
                                        node.hostPort.put(hostName, portNum);
                                        node.neighbours.put(Integer.valueOf(splitLine[0]), node.hostPort);
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
                else{
                        //Found some other Node's neighbour list
                        continue;
                }
            }

            //Create the object of receiver class which acts as an active server
            Receiver.serverInitialize(node.neighbours, node.currPortNum, node.currHostName);

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

class Receiver implements Runnable{
    private class message{
        //Structure of the messages that are created by each node to communicate
        private Integer sourceNodeID;
        private HashMap<Integer, Integer> nodesDiscovered;  //<NodeID, Hop count from the source node>
        private Stack<Integer> parentList;
    }
    Socket currentSocket;
    static Boolean serverOn = true;

    Receiver(Socket currentSocket) {
        this.currentSocket = currentSocket;
    }
    public static void serverInitialize(HashMap<Integer, HashMap<String, Integer>> neighbours, Integer currPortNum,
                                        String currHostName){
        ServerSocket serverSocket;
        try{
            //Initialize the receiver as a continuous listening server
            serverSocket = new ServerSocket(currPortNum);
            System.out.println("Listening");
            while (serverOn) {
                Socket sock = serverSocket.accept();
                System.out.println("Connected");
                new Thread(new Receiver(sock)).start();
            }
        } catch(Exception e){
            System.out.println("Could not create server on 1234 port number");
            e.printStackTrace();
        }

    }

    @Override
    public void run() {

    }
}

class Sender implements Runnable{
    Socket currentSocket;
    public static void senderInitialize(){

    }
    @Override
    public void run() {
        try {
            PrintStream printStream = new PrintStream(currentSocket.getOutputStream());
            for (int i = 100; i >= 0; i--) {
                printStream.println(i + " bottles of beer on the wall");
            }
            printStream.close();
            currentSocket.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}

class Processor{

}