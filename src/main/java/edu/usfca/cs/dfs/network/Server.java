package edu.usfca.cs.dfs.network;

import edu.usfca.cs.dfs.concurrent.WorkQueue;
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
    private WorkQueue workQueue;
    private volatile int numTasks = 0;

    public Server(String hostName, int port, Worker worker){
        this.hostName = hostName;
        this.port = port;
        this.worker = worker;
        workQueue = new WorkQueue(10);
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
                //System.out.println("Server: " + worker.getClass().getName());
                if (worker.getClass().getName().contains("ControllerWorker")) {
                    ControllerWorker newWorker = new ControllerWorker((ControllerWorker)worker);
                    newWorker.setSocket(socket);
                    workQueue.execute(newWorker);
                    //new Thread(newWorker).start();
                }
                else if (worker.getClass().getName().contains("StorageNodeWorker")){
                    StorageNodeWorker newWorker = new StorageNodeWorker((StorageNodeWorker)worker);
                    newWorker.setSocket(socket);
                    workQueue.execute(newWorker);
                    //new Thread(newWorker).start();
                }
                else{
                    System.out.println("Server: invalid worker class");
                    socket.close();
                }
            }catch (Exception e){
                e.printStackTrace();
                shutdown();
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
