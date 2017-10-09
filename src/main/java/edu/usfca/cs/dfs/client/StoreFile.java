package edu.usfca.cs.dfs.client;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.concurrent.ReentrantReadWriteLock;
import edu.usfca.cs.dfs.concurrent.WorkQueue;
import edu.usfca.cs.dfs.utilities.StorageMessages;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 * Created by tuo on 02/10/17.
 */
public class StoreFile {
    public static final int BUFFER_SIZE = 1024 * 1024; //The size of chunk's data
    private Socket clientForController;     //Communicate with Controller by this socket

    private List<StorageMessages.Node> getAvailableStorageNode(String filename, int index){
        List<StorageMessages.Node> nodeList = new LinkedList<>();
        try{
            this.clientForController = new Socket("bass01.cs.usfca.edu", 21000);
            StorageMessages.StoreFileMeta fileMeta =
                    StorageMessages.StoreFileMeta.newBuilder()
                            .setFileName(filename)
                            .setNumOfChunks(index)
                            .build();
            StorageMessages.StorageMessageWrapper msgWrapper =
                    StorageMessages.StorageMessageWrapper.newBuilder()
                            .setStoreFileMetaMsg(fileMeta)
                            .build();
            msgWrapper.writeDelimitedTo(clientForController.getOutputStream());
            msgWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(clientForController.getInputStream());
            if (msgWrapper.hasStoreFileMetaResponseMsg()){
                System.out.println("StoreFile: already get the storage node list");
                StorageMessages.StoreFileMetaResponse response = msgWrapper.getStoreFileMetaResponseMsg();
                nodeList = response.getNodeListList();
                for (StorageMessages.Node node : nodeList)
                    System.out.println("StoreFile: " + node.getHostname() + node.getPort());
            }
            this.clientForController.close();
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            return nodeList;
        }

    }

    /**
     * Split the file to create the chunk list before we send store request to Controller.
     * Update variable numOfChunks and chunkList. Send numOfChunks to Controller to get the
     * StorageNode list. Send the chunks in the Queue chunkList to those nodes by Thread Pool
     */
    private void createChunks(String filename){
        if (filename == null || filename.length() == 0){
            System.out.println("createChunks: Invalid filename");
            return ;
        }
        InputStream is = null;
        try{
            byte[] buffer = new byte[BUFFER_SIZE];
            is = new FileInputStream(filename);
            int readLen;
            for (int i = 0; (readLen = is.read(buffer)) != -1; i++){
                ByteString data;
                String md5;
                if (readLen == BUFFER_SIZE){
                    md5 = DigestUtils.md5Hex(buffer);
                    data = ByteString.copyFrom(buffer);
                }else {
                    byte[] newBuffer = new byte[readLen];
                    System.arraycopy(buffer, 0, newBuffer, 0, readLen);
                    md5 = DigestUtils.md5Hex(newBuffer);
                    data = ByteString.copyFrom(newBuffer);
                }
                StorageMessages.StoreChunk storeChunkMsg
                        = StorageMessages.StoreChunk.newBuilder()
                        .setFileName(filename)
                        .setChunkId(i)
                        .setMd5(md5)
                        .setData(data)
                        .build();
                LinkedList<StorageMessages.Node> nodeList = new LinkedList<>(getAvailableStorageNode(filename, i));
                sendChunkToStorageNodes(storeChunkMsg, nodeList);
                System.out.println("StoreFile: created chunk " + i);
                //chunkList.add(storeChunkMsg);
            }
            //this.numOfChunks = chunkList.size();
        } catch(Exception io){
            io.printStackTrace();
        } finally {
            try{
                if (is != null)
                    is.close();
            } catch(Exception io){
                io.printStackTrace();
            }
        }
    }

    /*
    * Store a file to storageNode.
    * 1. split this file to a list of chunks.
    * 2. talk with controller to get a list of available storage nodes.
    * 3. send chunks to storage nodes.
    * */
    public void storeFile(String filename){
        if (filename == null || filename.length() == 0){
            System.out.println("storeFile: invalid filename");
            return;
        }
        createChunks(filename);
        System.out.println("Client: We have saved this file to a bunch of storage nodes");
    }


    public void sendChunkToStorageNodes(StorageMessages.StoreChunk chunk, LinkedList<StorageMessages.Node> list){
        try {
            if (chunk != null) {
                StorageMessages.Node node = list.poll();
                Socket client = new Socket(node.getHostname(), node.getPort());
                StorageMessages.NodeList nodeList =
                        StorageMessages.NodeList.newBuilder()
                                .addAllList(list)
                                .build();
                StorageMessages.StoreChunk message =
                        chunk.toBuilder().setList(nodeList).build();
                StorageMessages.StorageMessageWrapper msgWrapper =
                        StorageMessages.StorageMessageWrapper.newBuilder()
                                .setStoreChunkMsg(message)
                                .build();
                msgWrapper.writeDelimitedTo(client.getOutputStream());
                client.close();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

}
