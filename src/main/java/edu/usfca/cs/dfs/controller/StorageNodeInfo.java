package edu.usfca.cs.dfs.controller;

import java.util.Date;

/**
 * Created by tuo on 26/09/17.
 */
public class StorageNodeInfo {
    public String hostName;
    public int port;
    public long timeStamp;

    public StorageNodeInfo(String hostName, int port, long timeStamp){
        this.hostName = hostName;
        this.port = port;
        this.timeStamp = timeStamp;
    }
}
