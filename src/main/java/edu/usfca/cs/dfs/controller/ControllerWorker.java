package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.network.NioServer;
import edu.usfca.cs.dfs.storageNode.StorageNode;
import edu.usfca.cs.dfs.utilities.StorageMessages;
import edu.usfca.cs.dfs.utilities.Worker;

import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Created by tuo on 26/09/17.
 */
public class ControllerWorker extends Worker{

    public Map<String, Map<Integer, List<StorageMessages.Node>>> mapOfChunkInfo;
    public Map<String, Map<String, List<Integer>>> detailOfStorageNode;
    public List<StorageNodeInfo> listOfStorageNode;

    public ControllerWorker(ControllerWorker worker){
        super(worker.hostName);
        this.mapOfChunkInfo = worker.mapOfChunkInfo;
        this.listOfStorageNode = worker.listOfStorageNode;
        this.detailOfStorageNode = worker.detailOfStorageNode;
    }

    public ControllerWorker(String hostName, Map<String, Map<Integer, List<StorageMessages.Node>>> mapOfChunkInfo
                                                , List<StorageNodeInfo> listOfStorageNode
                                                , Map<String, Map<String, List<Integer>>> detailOfStorageNode){
        super(hostName);
        this.mapOfChunkInfo = mapOfChunkInfo;
        this.listOfStorageNode = listOfStorageNode;
        this.detailOfStorageNode = detailOfStorageNode;
    }

    private List<StorageMessages.Node> createOrUpdate(){
        List<StorageMessages.Node> list = null;
        synchronized (mapOfChunkInfo){
            if (mapOfChunkInfo.containsKey(this.filename)
                && mapOfChunkInfo.get(this.filename).containsKey(this.chunkId)) {
                List<StorageMessages.Node> nodes = mapOfChunkInfo.get(filename).get(chunkId);
                if (nodes.size() > 0)
                    list = nodes;
            }

        }
        return list;
    }

    private void updateMapOfChunkInfo(List<StorageMessages.Node> list){
        synchronized (mapOfChunkInfo){
            if (!mapOfChunkInfo.containsKey(filename))
                mapOfChunkInfo.put(filename, new HashMap<Integer, List<StorageMessages.Node>>());
            Map<Integer, List<StorageMessages.Node>> map = mapOfChunkInfo.get(filename);
            if (!map.containsKey(chunkId))
                map.put(chunkId, new ArrayList<StorageMessages.Node>(list));
            else
                map.put(chunkId, list);
        }
    }

    private List<StorageMessages.Node> getStorageNodeList(StorageMessages.StoreFileMeta fileMeta){
        this.filename = fileMeta.getFileName();
        this.chunkId = fileMeta.getNumOfChunks();
        List<StorageMessages.Node> list = createOrUpdate();
        if (list != null)
            return list;
        list = getStorageNodes();
        //updateMapOfChunkInfo(list);
        return list;
    }

    private List<StorageMessages.Node> getStorageNodes(){
        List<StorageMessages.Node> list = new ArrayList<>();
        synchronized (listOfStorageNode){
            System.out.println("ControllerWorker: the number of storage node " + listOfStorageNode.size());
            if (listOfStorageNode.size() <= 3){
                for (StorageNodeInfo node : listOfStorageNode){
                    StorageMessages.Node sNode = StorageMessages.Node.newBuilder()
                            .setHostname(node.hostName)
                            .setPort(node.port)
                            .build();
                    list.add(sNode);
                }
            }else{
                int size = listOfStorageNode.size();
                Random r = new Random();
                int[] index = new int[3];
                int i = r.nextInt(size);
                Arrays.fill(index, i);
                while(index[1] == index[0])
                    index[1] = r.nextInt(size);
                while(index[2] == index[0] || index[2] == index[1])
                    index[2] = r.nextInt(size);
                for (i = 0; i < 3; i++){
                    StorageNodeInfo node = listOfStorageNode.get(index[i]);
                    StorageMessages.Node sNode = StorageMessages.Node.newBuilder()
                            .setHostname(node.hostName)
                            .setPort(node.port)
                            .build();
                    list.add(sNode);
                }
            }
        }
        return list;
    }

    private void getStorageNodeMeta(String nodeName, int port){
        StorageMessages.GetAllData message = StorageMessages.GetAllData.newBuilder().setTest(0).build();
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                .setGetAllDataMsg(message).build();
        try{
            System.out.println("ControllerWorker: try to get the metadata of this storage node");
            Socket client = new Socket(nodeName, port);
            msgWrapper.writeDelimitedTo(client.getOutputStream());
            //Thread.sleep(1000);
            StorageMessages.StorageMessageWrapper resWrapper = StorageMessages.
                    StorageMessageWrapper.parseDelimitedFrom(client.getInputStream());
            if (resWrapper.hasGetAllDataResponseMsg()){
                StorageMessages.GetAllDataResponse response = resWrapper.getGetAllDataResponseMsg();
                //System.out.println(response.toString());
                String host = response.getHostName();
                int newPort = response.getPort();
                List<StorageMessages.Chunk> list = response.getUpdateInfoList();
                System.out.println("ControllerWorker: get all the chunks info + " + host + newPort+ list.size());
                if (list.size() != 0){
                    System.out.println("ControllerWorker: already got the metadata of this storage node and it has chunks");
                    updateMapOfChunkInfo(list, host, newPort);
                    updateDetailOfStorageNode(list, host, newPort);
                }
            }
            client.close();
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    private void updateDetailOfStorageNode(List<StorageMessages.Chunk> list, String nodeName, int port){
        String key = nodeName + port;
        synchronized (detailOfStorageNode){
            System.out.println("ControllerWorker: trying to update DetailOfStorageNode");
            if (!detailOfStorageNode.containsKey(key))
                detailOfStorageNode.put(key, new HashMap<>());
            Map<String, List<Integer>> map = detailOfStorageNode.get(key);
            for (StorageMessages.Chunk chunk : list){
                String filename = chunk.getFilename();
                int chunkId = chunk.getChunkId();
                if (!map.containsKey(filename))
                    map.put(filename, new LinkedList<>());
                map.get(filename).add(chunkId);
            }
        }
    }

    private void updateListOfStorageNode(String nodeName, int port){
        long timeStamp = System.currentTimeMillis() / 1000;
        boolean exist = false;
        synchronized (listOfStorageNode) {
            List<StorageNodeInfo> diedNode = new ArrayList<>();
            for (StorageNodeInfo node : listOfStorageNode) {
                if (node.hostName.equals(nodeName) && node.port == port) {
                    exist = true;
                    node.timeStamp = timeStamp;
                } else if (timeStamp - node.timeStamp > 10)
                    diedNode.add(node);
            }
            for (StorageNodeInfo node : diedNode)
                listOfStorageNode.remove(node);
            if (!exist){
                listOfStorageNode.add(new StorageNodeInfo(nodeName, port, timeStamp));
                System.out.println("ControllerWorker: a new storage node added!");
                getStorageNodeMeta(nodeName, port);
            }
        }
    }

    private void updateMapOfChunkInfo(List<StorageMessages.Chunk> list, String nodeName, int port){
        synchronized (mapOfChunkInfo) {
            System.out.println("ControllerWorker: trying to update MapOfChunkInfo");
            for (StorageMessages.Chunk chunk : list) {
                String filename = chunk.getFilename();
                int chunkId = chunk.getChunkId();
                if (!mapOfChunkInfo.containsKey(filename)) {
                    mapOfChunkInfo.put(filename, new HashMap<Integer, List<StorageMessages.Node>>());
                }
                Map<Integer, List<StorageMessages.Node>> map = mapOfChunkInfo.get(filename);
                if (!map.containsKey(chunkId)) {
                    map.put(chunkId, new LinkedList<StorageMessages.Node>());
                }
                StorageMessages.Node node = StorageMessages.Node.newBuilder()
                        .setHostname(nodeName)
                        .setPort(port)
                        .build();
                map.get(chunkId).add(node);
            }
        }
    }

    private void getHeartBeatMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.HeartBeat message = msgWrapper.getHeartBeatMsg();
        String nodeName = message.getHostName();
        int port = message.getPort();
        List<StorageMessages.Chunk> list = message.getUpdateInfoList();
        System.out.println(Thread.currentThread().getId() + "ControllerWorker: get heartbeat message: " + nodeName + port);
        updateListOfStorageNode(nodeName, port);
        if (list.size() != 0){
            updateMapOfChunkInfo(list, nodeName, port);
            updateDetailOfStorageNode(list, nodeName, port);
        }
    }

    private void recoverChunk(StorageMessages.Node node, List<StorageMessages.Node> failedNodes, int id){
        Socket client = null;
        try{
            StorageMessages.RecoveryChunk message = StorageMessages.RecoveryChunk.newBuilder()
                    .setChunkId(id).setFilename(filename).addAllNodeList(failedNodes).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setRecoveryChunkMsg(message).build();
            client = new Socket(node.getHostname(), node.getPort());
            msgWrapper.writeDelimitedTo(client.getOutputStream());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                if (client != null)
                    client.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private boolean checkNode(StorageMessages.Node node, List<StorageMessages.Node> failedNodes, int id){
        Socket client = null;
        boolean result = false;
        try{
            StorageMessages.Chunk chunk = StorageMessages.Chunk.newBuilder().setChunkId(id).setFilename(filename).build();
            StorageMessages.CheckChunk message = StorageMessages.CheckChunk.newBuilder().setChunkInfo(chunk).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setCheckChunkMsg(message).build();
            client = new Socket(node.getHostname(), node.getPort());
            msgWrapper.writeDelimitedTo(client.getOutputStream());
            StorageMessages.StorageMessageWrapper resWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    client.getInputStream());
            if (resWrapper.hasCheckChunkResponseMsg()){
                result = resWrapper.getCheckChunkResponseMsg().getNodeOn();
                System.out.println("ControllerWorker: the result of check chunk " + id + " is " + result);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                if (client != null)
                    client.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            if (!result)
                failedNodes.add(node);
            return result;
        }
    }

    private List<StorageMessages.RetrieveNode> getRetrieveNodeList(){
        if (filename == null){
            System.out.println("ControllerWorker: invalid filename");
            return null;
        }
        synchronized (mapOfChunkInfo){
            if (mapOfChunkInfo.containsKey(filename)){
                List<StorageMessages.RetrieveNode> list = new LinkedList<>();
                Map<Integer, List<StorageMessages.Node>> map = mapOfChunkInfo.get(filename);
                int size = map.size();
                for (int i = 0; i < size; i++){
                    List<StorageMessages.Node> nodes = map.get(i);
                    if (!nodes.isEmpty()){
                        List<StorageMessages.Node> failedNodes = new LinkedList<>();
                        StorageMessages.Node node = null;
                        for (int index = 0; index < nodes.size(); index++){
                            node = nodes.get(index);
                            if (checkNode(node, failedNodes, i))
                                break;
                        }
                        if (node != null)
                            list.add(StorageMessages.RetrieveNode.newBuilder().setChunkId(i).setNode(node).build());
                        if (!failedNodes.isEmpty() && node != null)
                            recoverChunk(node, failedNodes, i);
                    }
                }
                return list;
            }
            return null;
        }

    }

    @Override
    public void run(){
        try{
            StorageMessages.StorageMessageWrapper msgWrapper
                    = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());
            if (msgWrapper.hasRetrieveFileMetaMsg()){
                this.filename = msgWrapper.getRetrieveFileMetaMsg().getFileName();
                List<StorageMessages.RetrieveNode> list = getRetrieveNodeList();
                StorageMessages.RetrieveFileMetaResponse response = StorageMessages.RetrieveFileMetaResponse
                        .newBuilder().addAllNodes(list).build();
                StorageMessages.StorageMessageWrapper resWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setRetrieveFileMetaResponseMsg(response).build();
                resWrapper.writeDelimitedTo(socket.getOutputStream());
            }
            else if (msgWrapper.hasHeartBeatMsg()){
                getHeartBeatMsg(msgWrapper);
            }
            else if (msgWrapper.hasStoreFileMetaMsg()){
                System.out.println(Thread.currentThread().getId() + " ControllerWorker: receive StoreFileMetaMsg");
                StorageMessages.StoreFileMeta message = msgWrapper.getStoreFileMetaMsg();
                List<StorageMessages.Node> list = getStorageNodeList(message);
                StorageMessages.StoreFileMetaResponse.Builder builder =
                        StorageMessages.StoreFileMetaResponse.newBuilder();
                builder.addAllNodeList(list);
                StorageMessages.StoreFileMetaResponse response = builder.build();
                StorageMessages.StorageMessageWrapper resWrapper =
                        StorageMessages.StorageMessageWrapper.newBuilder()
                        .setStoreFileMetaResponseMsg(response)
                        .build();
                resWrapper.writeDelimitedTo(this.socket.getOutputStream());
                System.out.println(Thread.currentThread().getId() + " ControllerWorker: send StoreFileMetaMsgResponse");
            }
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void getStoreChunkMsg(StorageMessages.StorageMessageWrapper msgWrapper){}
    public void getRetrieveFileMsg(StorageMessages.StorageMessageWrapper msgWrapper){}
    public void processData(NioServer server, SocketChannel socket, byte[] data, int count){}
}
