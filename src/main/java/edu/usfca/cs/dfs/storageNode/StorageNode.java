package edu.usfca.cs.dfs.storageNode;


import edu.usfca.cs.dfs.utilities.StorageMessages;
import edu.usfca.cs.dfs.network.Server;
import edu.usfca.cs.dfs.utilities.Chunk;

import java.net.*;
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
    private static final String DATA_PATH = "./data";
    private List<StorageMessages.Chunk> recentChanges;


    public StorageNode(String hostName, int port){
        chunkMap = new HashMap<String, Map<Integer, Chunk>>();
        recentChanges = new ArrayList<>();
        this.port = port;
        this.hostName = hostName;
    }

    public void initialChunkMap(){
        LoadInfoFromDisk loadInfoFromDisk = new LoadInfoFromDisk(chunkMap);
        loadInfoFromDisk.loadInfo(Paths.get(DATA_PATH));
    }

    public void initialRecentChanges(){
        if (chunkMap.isEmpty()){
            System.out.println("StorageNode: Currently, this node have no chunk been stored");
        }
        for (Map.Entry<String, Map<Integer, Chunk>> entry : chunkMap.entrySet()){
            String filename = entry.getKey();
            for (Map.Entry<Integer, Chunk> chunkEntry : entry.getValue().entrySet()){
                int chunkId = chunkEntry.getKey();
                recentChanges.add(StorageMessages.Chunk.newBuilder()
                                    .setFilename(filename)
                                    .setChunkId(chunkId).build());
            }
        }
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

    private ServerSocket srvSocket;

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            return;
        StorageNode node = new StorageNode(getHostname(), Integer.valueOf(args[1]));
        //StorageNode node = new StorageNode(getHostname(), 3000);
        System.out.println("StorageNode " + node.hostName + "initial chunk map");
        node.initialChunkMap();
        node.initialRecentChanges();
        System.out.println("StorageNode " + node.hostName + "start send HeatBeat");
        node.HeartBeat();
        StorageNodeWorker worker = new StorageNodeWorker(node.hostName, node.recentChanges);
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
