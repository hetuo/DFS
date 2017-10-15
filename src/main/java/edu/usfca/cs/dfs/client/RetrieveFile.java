package edu.usfca.cs.dfs.client;

import edu.usfca.cs.dfs.concurrent.ReentrantReadWriteLock;
import edu.usfca.cs.dfs.concurrent.WorkQueue;
import edu.usfca.cs.dfs.utilities.StorageMessages;

import java.io.FileOutputStream;
import java.net.Socket;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;

/**
 * Created by tuo on 04/10/17.
 */
public class RetrieveFile {

    private Socket clientForController;     //Communicate with Controller by this socket
    private String filename;                //The file's name for processing
    private LinkedList<StorageMessages.RetrieveNode> storageNodeList = null;  //A list of storage nodes for storing chunks
    private WorkQueue workQueue = new WorkQueue(20);        //Work queue for sending chunks
    private volatile int numTasks = 0;                              //Shutdown work queue if this variable equals 0
    private ReentrantReadWriteLock lock;                //For synchronous reading chunk list
    private PriorityQueue<StorageMessages.RetrieveChunkResponse> chunkList;
    private volatile int nextChunkId = 0;
    private FileOutputStream file;


    public RetrieveFile(String filename){
        this.filename = filename;
        lock = new ReentrantReadWriteLock();
        chunkList = new PriorityQueue<StorageMessages.RetrieveChunkResponse>(100, new Comparator<StorageMessages.RetrieveChunkResponse>() {
            @Override
            public int compare(StorageMessages.RetrieveChunkResponse r1,
                               StorageMessages.RetrieveChunkResponse r2) {
                return r1.getChunkId() - r2.getChunkId();
            }
        });
    }

    public void getStorageNodes(String filename){
        try{
            this.clientForController = new Socket("bass01.cs.usfca.edu", 21000);
            StorageMessages.RetrieveFileMeta fileMeta = StorageMessages.RetrieveFileMeta.newBuilder()
                    .setFileName(filename)
                    .build();

            StorageMessages.StorageMessageWrapper msgWrapper =
                    StorageMessages.StorageMessageWrapper.newBuilder()
                            .setRetrieveFileMetaMsg(fileMeta)
                            .build();
            msgWrapper.writeDelimitedTo(clientForController.getOutputStream());
            msgWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(clientForController.getInputStream());
            if (msgWrapper.hasRetrieveFileMetaResponseMsg()){
                StorageMessages.RetrieveFileMetaResponse response = msgWrapper.getRetrieveFileMetaResponseMsg();
                storageNodeList = new LinkedList<StorageMessages.RetrieveNode>(response.getNodesList());
            }
            this.clientForController.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void retrieveFile(String filename){
        if (filename == null || filename.length() == 0){
            System.out.println("RetrieveFile: invalid filename");
            return;
        }
        getStorageNodes(filename);
        if (storageNodeList != null) {
            try {
                file = new FileOutputStream("new" + filename);
                workQueue.execute(new sendHandler());
                shutdown();
                while (!chunkList.isEmpty()){
                    StorageMessages.RetrieveChunkResponse chunk = chunkList.poll();
                    System.out.println("RetrieveFile: going to write chunk " + chunk.getChunkId());
                    byte[] data = chunk.getData().toByteArray();
                    file.write(data);
                }
                file.flush();
                file.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    public void addChunkToList(StorageMessages.RetrieveChunkResponse response){
        lock.lockWrite();
        try {
            chunkList.add(response);
            System.out.println("RetrieveFile: added chunk " + response.getChunkId());
            while (!chunkList.isEmpty() && chunkList.peek().getChunkId() == nextChunkId){
                StorageMessages.RetrieveChunkResponse chunk = chunkList.poll();
                byte[] data = chunk.getData().toByteArray();
                System.out.println("RetrieveFile: going to write chunk " + chunk.getChunkId());
                file.write(data);
                nextChunkId++;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            lock.unlockWrite();
        }
    }

    private class sendHandler implements Runnable{

        public sendHandler(){
            incrementTasks();
        }
        @Override
        public void run(){
            try {
                StorageMessages.RetrieveNode node = storageNodeList.poll();
                if (storageNodeList.size() != 0)
                    workQueue.execute(new RetrieveFile.sendHandler());
                int chunkId = node.getChunkId();
                StorageMessages.Node nodeInfo = node.getNode();
                StorageMessages.RetrieveChunk request = StorageMessages.RetrieveChunk.newBuilder()
                        .setChunkId(chunkId)
                        .setFilename(filename)
                        .build();
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder()
                        .setRetrieveChunkMsg(request)
                        .build();
                Socket client = new Socket(nodeInfo.getHostname(), nodeInfo.getPort());
                msgWrapper.writeDelimitedTo(client.getOutputStream());
                msgWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        client.getInputStream());
                if (msgWrapper.hasRetrieveChunkResponseMsg()){
                    addChunkToList(msgWrapper.getRetrieveChunkResponseMsg());
                }
                client.close();
            } catch (Exception e){
                e.printStackTrace();
            } finally {
                decrementTasks();
            }
        }
    }

    /** Increment the number of tasks */
    public synchronized void incrementTasks()
    {
        numTasks++;
    }

    /** Decrement the number of tasks.
     * Call notifyAll() if no pending work left.
     */
    public synchronized void decrementTasks()
    {
        numTasks--;
        if (numTasks <= 0)
            notifyAll();
    }

    /**
     * Wait until there is no pending work, then shutdown the queue
     */
    public synchronized void shutdown()
    {
        waitUntilFinished();
        workQueue.shutdown();
        workQueue.awaitTermination();
    }

    /**
     *  Wait for all pending work to finish
     */
    public synchronized void waitUntilFinished() {
        while (numTasks > 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
