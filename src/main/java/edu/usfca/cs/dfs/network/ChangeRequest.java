package edu.usfca.cs.dfs.network;

import java.nio.channels.SocketChannel;

/**
 * Created by tuo on 23/09/17.
 */
public class ChangeRequest {
    public SocketChannel socket;
    public int ops;

    public ChangeRequest(SocketChannel socket, int ops){
        this.socket = socket;
        this.ops = ops;
    }
}
