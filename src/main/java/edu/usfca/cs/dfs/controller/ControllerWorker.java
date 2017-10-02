package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.network.NioServer;
import edu.usfca.cs.dfs.utilities.StorageMessages;
import edu.usfca.cs.dfs.utilities.Worker;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by tuo on 26/09/17.
 */
public class ControllerWorker extends Worker{

    public Map<String, Map<Integer, List<StorageNodeInfo>>> mapOfChunkInfo;
    public Map<String, StorageNodeInfo> mapOfStorageNode;

    public ControllerWorker(String hostName, Map<String, Map<Integer, List<StorageNodeInfo>>> mapOfChunkInfo
                                                , Map<String, StorageNodeInfo> mapOfStorageNode){
        super(hostName);
        this.mapOfChunkInfo = mapOfChunkInfo;
        this.mapOfStorageNode = mapOfStorageNode;
    }

    @Override
    public void getStoreChunkMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.StoreChunk chunk = msgWrapper.getStoreChunkMsg();
        this.filename = chunk.getFileName();
        this.chunkId = chunk.getChunkId();
        //this.md5 = chunk.getMd5();
        //data = chunk.getData().toByteArray();
    }

    private List<StorageNodeInfo> getStorageNodes(){
        List<StorageNodeInfo> list = new ArrayList<>();
        synchronized (mapOfStorageNode){
            /*if (mapOfStorageNode.containsKey("localhost"))
                list.add(mapOfStorageNode.get("localhost"));*/
            for (Map.Entry<String, StorageNodeInfo> entry : mapOfStorageNode.entrySet())
                list.add(entry.getValue());
            return list;
        }
    }

    private void getHeartBeatMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.HeartBeat message = msgWrapper.getHeartBeatMsg();
        String nodeName = message.getHostName();
        int port = message.getPort();
        List<String> list = message.getUpdateInfoList();
        System.out.println("ControllerWorker: get heartbeat message: " + nodeName);
        synchronized (mapOfStorageNode){
            if (mapOfStorageNode.containsKey(nodeName))
                mapOfStorageNode.get(nodeName).isActive = true;
            else{
                StorageNodeInfo node = new StorageNodeInfo(nodeName, port);
                mapOfStorageNode.put(nodeName, node);
            }
        }
        if (list.size() != 0){
            // update map of chunks information
        }
    }

    public void getRetrieveFileMsg(StorageMessages.StorageMessageWrapper msgWrapper){}
    public void processData(NioServer server, SocketChannel socket, byte[] data, int count){}

    @Override
    public void run(){
        try{
            StorageMessages.StorageMessageWrapper msgWrapper
                    = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());
            if (msgWrapper.hasStoreChunkMsg()){
                System.out.println("ControllerWorker: get store chunk message");
                getStoreChunkMsg(msgWrapper);
                List<StorageNodeInfo> list = getStorageNodes();
                List<String> nodeList = new ArrayList<>();
                for (StorageNodeInfo node : list){
                    nodeList.add(node.hostName + "-" + node.port);
                }
                StorageMessages.StoreChunkResponse.Builder messageBuilder = StorageMessages.StoreChunkResponse.newBuilder();
                messageBuilder.setFileName(this.filename);
                messageBuilder.setChunkId(this.chunkId);
                if (nodeList.size() != 0)
                    messageBuilder.addAllNodeList(nodeList);
                StorageMessages.StoreChunkResponse message = messageBuilder.build();
                StorageMessages.StorageMessageWrapper responseWrapper =
                        StorageMessages.StorageMessageWrapper.newBuilder()
                        .setStoreChunkResponseMSg(message).build();
                System.out.println("ControllerWorker: ready to send StoreChunkResponse");
                responseWrapper.writeDelimitedTo(socket.getOutputStream());
            }
            else if (msgWrapper.hasRetrieveFileMsg()){

            }
            else if (msgWrapper.hasHeartBeatMsg()){
                getHeartBeatMsg(msgWrapper);
            }
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
