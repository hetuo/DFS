package edu.usfca.cs.dfs.network;




import edu.usfca.cs.dfs.utilities.Worker;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * Created by tuo on 23/09/17.
 */
public class NioServer implements Runnable{

    //public static final int BUFFER_SIZE = 4;
    private InetAddress hostAddress;
    private int port;
    private ServerSocketChannel serverChannel;
    private Selector selector;
    private ByteBuffer readBuffer = ByteBuffer.allocate(512);
    private Worker worker;
    private Map<SocketChannel, ChangeRequest> pendingData = new HashMap<>();
    private List<ChangeRequest> pendingChanges = new LinkedList<>();

    public NioServer(InetAddress hostAddress, int port, Worker worker) throws IOException{
        this.hostAddress = hostAddress;
        this.port = port;
        this.worker = worker;
        this.selector = initSelector();
    }

    private Selector initSelector() throws IOException{
        Selector selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        InetSocketAddress isa = new InetSocketAddress(hostAddress, port);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        return selector;
    }

    private void accept(SelectionKey key) throws IOException{
        //For an accept to be pending the channel must be a server socket channel.
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        // Accept the connection and make it non-blocking
        SocketChannel socketChannel = serverSocketChannel.accept();
        Socket socket = socketChannel.socket();
        socketChannel.configureBlocking(false);
        // Register the new SocketChannel with our Selector, indicating
        // we'd like to be notified when there's data waiting to be read
        socketChannel.register(this.selector, SelectionKey.OP_READ);
    }

    private void read(SelectionKey key) throws IOException{
        SocketChannel socketChannel = (SocketChannel) key.channel();
        readBuffer.clear();
        // Attempt to read off the channel
        int numRead;
        try {
            numRead = socketChannel.read(this.readBuffer);
        } catch (IOException e) {
            // The remote closed the connection, cancel
            // the selection key and close the channel.
            key.cancel();
            socketChannel.close();
            return;
        }
        if (numRead == -1) {
            // Remote shut the socket down. Do the
            // same from our end and cancel the channel.
            key.channel().close();
            key.cancel();
            return;
        }

        // Hand the data off to our worker thread
        this.worker.processData(this, socketChannel, this.readBuffer.array(), numRead);
    }

    private void write(SelectionKey key) throws IOException{
        SocketChannel socketChannel = (SocketChannel) key.channel();
        synchronized (pendingData){
            //ChangeRequest
        }
    }

    private void send(SocketChannel socket, com.google.protobuf.GeneratedMessageV3 data){
        synchronized (pendingChanges){
            pendingChanges.add(new ChangeRequest(socket, SelectionKey.OP_WRITE));
            synchronized (pendingData){

            }
        }
    }

    public void run(){
        while (true){
            try{
                selector.select();
                Iterator selectedKeys = selector.selectedKeys().iterator();
                while (selectedKeys.hasNext()){
                    SelectionKey key = (SelectionKey)selectedKeys.next();
                    selectedKeys.remove();
                    if (!key.isValid()) continue;
                    if (key.isAcceptable()) accept(key);
                    else if (key.isReadable()) read(key);
                    else if (key.isWritable()) write(key);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

}
