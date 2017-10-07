package edu.usfca.cs.dfs.storageNode;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.utilities.Chunk;
import edu.usfca.cs.dfs.utilities.StorageMessages;
import edu.usfca.cs.dfs.network.NioServer;
import edu.usfca.cs.dfs.utilities.Worker;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by tuo on 25/09/17.
 */
public class StorageNodeWorker extends Worker{

    public List<StorageMessages.Chunk> recentChanges;
    private LinkedList<StorageMessages.Node> nodeList;
    private int port;

    public StorageNodeWorker(String hostName, List<StorageMessages.Chunk> recentChanges, int port){
        super(hostName);
        this.recentChanges = recentChanges;
        this.port = port;
    }

    public StorageNodeWorker(StorageNodeWorker worker){
        super(worker.hostName);
        this.recentChanges = worker.recentChanges;
        this.port = worker.port;
    }

    @Override
    public void getStoreChunkMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.StoreChunk chunk = msgWrapper.getStoreChunkMsg();
        this.nodeList = new LinkedList<>(chunk.getList().getListList());
        System.out.println("StorageNodeWorker: size of nodeList " + nodeList.size());
        this.filename = chunk.getFileName();
        this.chunkId = chunk.getChunkId();
        this.md5 = chunk.getMd5();
        data = chunk.getData().toByteArray();
    }

    public void getCheckChunkMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.CheckChunk message = msgWrapper.getCheckChunkMsg();
        this.filename = message.getChunkInfo().getFilename();
        this.chunkId = message.getChunkInfo().getChunkId();
    }

    public void getRecoveryChunkMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.RecoveryChunk message = msgWrapper.getRecoveryChunkMsg();
        this.filename = message.getFilename();
        this.chunkId = message.getChunkId();
        this.nodeList = new LinkedList<>(message.getNodeListList());
    }

    public void storeChunk() throws Exception{
        String path = "./data"+port+"/" + this.filename + "/" + this.md5 + "-" + this.chunkId;
        File f = new File(path);
        f.getParentFile().mkdirs();
        f.createNewFile();

        FileOutputStream fs = new FileOutputStream(path);
        fs.write(this.data);
        fs.flush();
        fs.close();
    }

    public void updateRecentChanges(){
        synchronized (recentChanges){
            recentChanges.add(StorageMessages.Chunk.newBuilder()
                                .setFilename(this.filename)
                                .setChunkId(this.chunkId).build());
        }
    }

    public void sendChunkToAnotherNode(){
        StorageMessages.Node node = nodeList.poll();
        StorageMessages.StoreChunk chunk = StorageMessages.StoreChunk.newBuilder()
                .setChunkId(chunkId)
                .setFileName(filename)
                .setMd5(md5)
                .setList(StorageMessages.NodeList.newBuilder()
                        .addAllList(nodeList).build())
                .setData(ByteString.copyFrom(data))
                .build();
        try{
            Socket client = new Socket(node.getHostname(), node.getPort());
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder()
                    .setStoreChunkMsg(chunk)
                    .build();
            msgWrapper.writeDelimitedTo(client.getOutputStream());
            client.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void getRetrieveFileMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.StoreChunk chunk = msgWrapper.getStoreChunkMsg();
        this.filename = chunk.getFileName();
    }

    public void getRetrieveChunkMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.RetrieveChunk chunk = msgWrapper.getRetrieveChunkMsg();
        this.filename = chunk.getFilename();
        this.chunkId = chunk.getChunkId();
    }

    public byte[] readAndCheckChunk(){
        FileInputStream is = null;
        byte[] data = null;
        try{
            for (Path path : Files.newDirectoryStream(Paths.get("./data"+port+"/" + filename))){
                if (path.toString().endsWith("-" + chunkId)){
                    String[] strings = path.toString().split("/");
                    String md5sum = strings[strings.length - 1].substring(0, 32);
                    is = new FileInputStream(path.toString());
                    data = new byte[(int)(new File(path.toString()).length())];
                    is.read(data);
                    System.out.println("StorageNodeWorker: " + DigestUtils.md5Hex(data) + ":" + md5sum);
                    if (!md5sum.equals(DigestUtils.md5Hex(data))){
                        data = null;
                        //need to delete this broken chunk;
                        Files.delete(path);
                    }else{
                        System.out.println("StorageNodeWorker: get the right file");
                        this.md5 = new String(md5sum);
                        this.data = data;
                    }
                    break;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (is != null)
                    is.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            return data;
        }
    }

    public StorageMessages.RetrieveChunkResponse retrieveChunk(){
        StorageMessages.RetrieveChunkResponse response = null;
        byte[] data = readAndCheckChunk();
        System.out.println("StorageNodeWorker: data is null?");
        if (data != null){
            System.out.println("StorageNodeWorker: no");
            response = StorageMessages.RetrieveChunkResponse.newBuilder()
                    .setChunkId(chunkId).setFilename(filename)
                    .setData(ByteString.copyFrom(data)).build();
        }
        return response;
    }


    public void getAllData(List<StorageMessages.Chunk> list){
        File f = new File(StorageNode.DATA_PATH);
        if (!f.exists()){
            System.out.println("StorageNodeWorker: " + StorageNode.DATA_PATH + " doesn't exist");
            return;
        }
        LoadInfoFromDisk loader = new LoadInfoFromDisk(list);
        loader.loadInfo(Paths.get(StorageNode.DATA_PATH));
    }

    @Override
    public void run(){
        try{
            StorageMessages.StorageMessageWrapper msgWrapper
                    = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());
            if (msgWrapper.hasStoreChunkMsg()){
                getStoreChunkMsg(msgWrapper);
                storeChunk();
                updateRecentChanges();
                if (nodeList != null && nodeList.size() != 0) {
                    System.out.println("StorageNodeWorker: going to send data to another node");
                    sendChunkToAnotherNode();
                }
            }
            if (msgWrapper.hasGetAllDataMsg()){
                System.out.println("StorageNodeWorker: received get all data request");
                List<StorageMessages.Chunk> list = new ArrayList<>();
                getAllData(list);
                System.out.println("StorageNodeWorker: this size of data is: " + list.size()+this.hostName+this.port);
                StorageMessages.GetAllDataResponse response = StorageMessages.GetAllDataResponse.newBuilder()
                        .setHostName(this.hostName).setPort(this.port).addAllUpdateInfo(list).build();

                StorageMessages.StorageMessageWrapper resWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setGetAllDataResponseMsg(response).build();
                System.out.println("StorageNodeWorker: going to send data back");

                resWrapper.writeDelimitedTo(socket.getOutputStream());

            }
            if (msgWrapper.hasRetrieveChunkMsg()){
                getRetrieveChunkMsg(msgWrapper);
                StorageMessages.RetrieveChunkResponse response = retrieveChunk();
                StorageMessages.StorageMessageWrapper resWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setRetrieveChunkResponseMsg(response).build();
                resWrapper.writeDelimitedTo(socket.getOutputStream());
            }
            if (msgWrapper.hasRecoveryChunkMsg()){
                getRecoveryChunkMsg(msgWrapper);
                readAndCheckChunk();
                if (nodeList != null && nodeList.size() != 0) {
                    System.out.println("StorageNodeWorker: going to send data to another node");
                    sendChunkToAnotherNode();
                }
            }
            if (msgWrapper.hasCheckChunkMsg()){
                getCheckChunkMsg(msgWrapper);
                byte[] data = readAndCheckChunk();
                System.out.println("StorageNodeWoker: going to send response to Controller for chunk checking");
                StorageMessages.CheckChunkResponse response = null;
                if (data == null)
                    response = StorageMessages.CheckChunkResponse.newBuilder().setNodeOn(false).build();
                else
                    response = StorageMessages.CheckChunkResponse.newBuilder().setNodeOn(true).build();
                StorageMessages.StorageMessageWrapper resWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setCheckChunkResponseMsg(response).build();
                resWrapper.writeDelimitedTo(socket.getOutputStream());
            }
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void processData(NioServer server, SocketChannel socket, byte[] data, int count){}

}
