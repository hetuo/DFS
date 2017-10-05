package edu.usfca.cs.dfs.network;

import edu.usfca.cs.dfs.controller.ControllerWorker;
import edu.usfca.cs.dfs.storageNode.StorageNodeWorker;
import edu.usfca.cs.dfs.utilities.Worker;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

/**
 * Created by tuo on 25/09/17.
 */
public class Server implements Runnable{

    private String hostName;
    private int port;
    private Worker worker;

    public Server(String hostName, int port, Worker worker){
        this.hostName = hostName;
        this.port = port;
        this.worker = worker;
    }

    public void run(){
        ServerSocket serverSocket = null;
        Socket socket = null;
        try{
            serverSocket = new ServerSocket(port);
        }catch (Exception e){
            e.printStackTrace();
        }
        while (true){
            try{
                socket = serverSocket.accept();
                System.out.println("Server: " + worker.getClass().getName());
                if (worker.getClass().getName().contains("ControllerWorker")) {
                    ControllerWorker newWorker = new ControllerWorker((ControllerWorker)worker);
                    newWorker.setSocket(socket);
                    new Thread(newWorker).start();
                }
                else if (worker.getClass().getName().contains("StorageNodeWorker")){
                    StorageNodeWorker newWorker = new StorageNodeWorker((StorageNodeWorker)worker);
                    newWorker.setSocket(socket);
                    new Thread(newWorker).start();
                }
                else{
                    System.out.println("Server: invalid worker class");
                    socket.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }

    }


}
