package edu.usfca.cs.dfs.utilities;

import edu.usfca.cs.dfs.utilities.StorageMessages;
import edu.usfca.cs.dfs.network.NioServer;

import java.net.Socket;
import java.nio.channels.SocketChannel;

/**
 * Created by tuo on 23/09/17.
 */
public abstract  class Worker implements Runnable{

    public String hostName;
    public Socket socket;
    public int chunkId;
    public String md5;
    public String filename;
    public byte[] data;


    public Worker(String hostName){this.hostName = hostName;}

    public void setSocket(Socket socket){
        this.socket = socket;
    }

    public abstract void getStoreChunkMsg(StorageMessages.StorageMessageWrapper msgWrapper);
    public abstract void getRetrieveFileMsg(StorageMessages.StorageMessageWrapper msgWrapper);
    public abstract void processData(NioServer server, SocketChannel socket, byte[] data, int count);
}
