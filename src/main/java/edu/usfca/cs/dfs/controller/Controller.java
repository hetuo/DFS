package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.network.Server;
import edu.usfca.cs.dfs.utilities.StorageMessages;

import java.util.*;

public class Controller {

    // Record the live storage node in this system
    //private Map<String, StorageNodeMeta> mapOfStorageNode;
    // Use priority queue to get the most suitable storage node for client, this queue should update together
    // with mapOfStorageNode
    PriorityQueue<StorageNodeMeta> queue;
    // Record the chunks' location of files which stored in this system
    public Map<String, List<List<String>>> mapOfFile;
    public final static int PORT = 8000;
    public final static String HOSTNAME = "localhost";
    public Map<String, Map<Integer, List<StorageMessages.Node>>> mapOfChunkInfo;
    //public Map<String, StorageNodeInfo> mapOfStorageNode;
    public List<StorageNodeInfo> listOfStorageNode;

    public Controller(){
        mapOfChunkInfo = new HashMap<String, Map<Integer, List<StorageMessages.Node>>>();
        //mapOfStorageNode = new HashMap<String, StorageNodeInfo>();
        listOfStorageNode = new LinkedList<>();
    }


    /*public Controller(){
        mapOfFile = new HashMap<>();
        queue = new PriorityQueue<>(new Comparator<StorageNodeMeta>() {
            @Override
            public int compare(StorageNodeMeta node1, StorageNodeMeta node2) {
                return node1.getNumOfChunks() - node2.getNumOfChunks();
            }
        });
        mapOfStorageNode = new HashMap<>();
    }*/

    public void startServer(){

    }




    public static void main(String[] args) throws Exception{
        Controller controller = new Controller();
        ControllerWorker worker = new ControllerWorker(HOSTNAME, controller.mapOfChunkInfo, controller.listOfStorageNode);
        Server server = new Server(controller.HOSTNAME, controller.PORT, worker);
        System.out.println("Controller " + controller.HOSTNAME + "start listen socket connection");
        Thread thread = new Thread(server);
        thread.start();
        thread.join();
    }


}
