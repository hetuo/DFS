package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.utilities.StorageMessages;
import edu.usfca.cs.dfs.network.NioServer;
import edu.usfca.cs.dfs.utilities.Worker;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.SocketChannel;
import java.util.Map;

/**
 * Created by tuo on 25/09/17.
 */
public class StorageNodeWorker extends Worker{

    public Map<String, Integer> recentChanges;

    public StorageNodeWorker(String hostName, Map<String, Integer> recentChanges){
        super(hostName);
        this.recentChanges = recentChanges;
    }

    @Override
    public void processData(NioServer server, SocketChannel socket, byte[] data, int count){}

    @Override
    public void getStoreChunkMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.StoreChunk chunk = msgWrapper.getStoreChunkMsg();
        this.filename = chunk.getFileName();
        this.chunkId = chunk.getChunkId();
        this.md5 = chunk.getMd5();
        data = chunk.getData().toByteArray();
    }

    public void storeChunk() throws Exception{
        String path = "./data/" + this.filename + "/" + this.md5 + "-" + this.chunkId;
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
            recentChanges.put(this.filename, this.chunkId);
        }
    }


    @Override
    public void getRetrieveFileMsg(StorageMessages.StorageMessageWrapper msgWrapper){
        StorageMessages.StoreChunk chunk = msgWrapper.getStoreChunkMsg();
        this.filename = chunk.getFileName();
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
            }
            if (msgWrapper.hasRetrieveFileMsg()){

            }
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
