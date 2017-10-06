package edu.usfca.cs.dfs.storageNode;


import edu.usfca.cs.dfs.utilities.StorageMessages;
import edu.usfca.cs.dfs.network.Server;
import edu.usfca.cs.dfs.utilities.Chunk;

import java.io.File;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StorageNode {

    private int port;
    private String hostName;
    private String nameNodeAddr = "localhost";
    private int nameNodePort = 8000;
    private Map<String, Map<Integer, Chunk>> chunkMap;
    public static String DATA_PATH;
    private List<StorageMessages.Chunk> recentChanges;


    public StorageNode(String hostName, int port){
        chunkMap = new HashMap<String, Map<Integer, Chunk>>();
        recentChanges = new ArrayList<>();
        this.port = port;
        this.hostName = hostName;
        DATA_PATH = "./data" + port + "/";
    }

    public void clearDisk(){
        ClearDisk clear = new ClearDisk();
        clear.deleteDirectory(Paths.get(DATA_PATH));
    }

    public void HeartBeat(){
        Runnable task = new Runnable() {
            public void run(){
                try{
                    synchronized (recentChanges){
                        List<StorageMessages.Chunk> list = new ArrayList<>(recentChanges);
                        Socket client = new Socket(nameNodeAddr, nameNodePort);
                        StorageMessages.HeartBeat.Builder messageBuilder = StorageMessages.HeartBeat.newBuilder();
                        messageBuilder.addAllUpdateInfo(list);
                        messageBuilder.setHostName(hostName);
                        messageBuilder.setPort(port);
                        StorageMessages.HeartBeat message = messageBuilder.build();
                        StorageMessages.StorageMessageWrapper msgWrapper =
                                StorageMessages.StorageMessageWrapper.newBuilder()
                                    .setHeartBeatMsg(message)
                                    .build();
                        msgWrapper.writeDelimitedTo(client.getOutputStream());
                        recentChanges.clear();
                        client.close();
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        };

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(task, 1, 5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws Exception {
        //if (args.length != 1)
        //    return;
        //StorageNode node = new StorageNode(getHostname(), Integer.valueOf(args[0]));
        Scanner sc = new Scanner(System.in);
        System.out.println("Please enter the port number: ");
        int port = sc.nextInt();
        StorageNode node = new StorageNode(getHostname(), port);
        System.out.println("StorageNode " + node.hostName + "initial chunk map");
        //node.initialChunkMap();
        //node.initialRecentChanges();
        node.clearDisk();
        System.out.println("StorageNode " + node.hostName + "start send HeatBeat");
        node.HeartBeat();
        StorageNodeWorker worker = new StorageNodeWorker(node.hostName, node.recentChanges, port);
        Server server = new Server(node.hostName, node.port, worker);
        System.out.println("StorageNode " + node.hostName + "start listen socket connection");
        Thread thread = new Thread(server);
        thread.start();
        thread.join();
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
            throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
