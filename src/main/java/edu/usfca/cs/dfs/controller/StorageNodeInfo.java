package edu.usfca.cs.dfs.controller;

/**
 * Created by tuo on 26/09/17.
 */
public class StorageNodeInfo {
    public String hostName;
    public int port;
    public boolean isActive;

    public StorageNodeInfo(String hostName, int port){
        this.hostName = hostName;
        this.port = port;
        this.isActive = true;
    }
}
