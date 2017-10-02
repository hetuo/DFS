package edu.usfca.cs.dfs.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

/**
 * Created by tuo on 23/09/17.
 */
public class NioSelector {

    private Selector selector;
    private ServerSocketChannel serverSocket;

    public NioSelector(String hostname, int port) throws IOException{
        selector = Selector.open();
        serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress(hostname, port));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
    }
}
