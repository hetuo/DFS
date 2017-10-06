package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.network.Server;
import edu.usfca.cs.dfs.utilities.StorageMessages;

import java.util.*;

public class Controller {

    public final static int PORT = 8000;
    public final static String HOSTNAME = "localhost";
    public Map<String, Map<Integer, List<StorageMessages.Node>>> mapOfChunkInfo;
    public List<StorageNodeInfo> listOfStorageNode;
    public Map<String, Map<String, List<Integer>>> detailOfStorageNode;

    public Controller(){
        mapOfChunkInfo = new HashMap<String, Map<Integer, List<StorageMessages.Node>>>();
        detailOfStorageNode = new HashMap<>();
        listOfStorageNode = new LinkedList<>();
    }

    public static void main(String[] args) throws Exception{
        Controller controller = new Controller();
        ControllerWorker worker = new ControllerWorker(HOSTNAME, controller.mapOfChunkInfo,
                controller.listOfStorageNode, controller.detailOfStorageNode);
        Server server = new Server(controller.HOSTNAME, controller.PORT, worker);
        System.out.println("Controller " + controller.HOSTNAME + "start listen socket connection");
        Thread thread = new Thread(server);
        thread.start();
        thread.join();
    }


}
