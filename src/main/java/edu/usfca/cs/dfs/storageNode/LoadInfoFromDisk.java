package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.concurrent.ReentrantReadWriteLock;
import edu.usfca.cs.dfs.concurrent.WorkQueue;
import edu.usfca.cs.dfs.utilities.Chunk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tuo on 22/09/17.
 */
public class LoadInfoFromDisk {

    private WorkQueue queue = new WorkQueue();
    private volatile int numTasks = 0;
    private Map<String, Map<Integer, Chunk>> chunkMap;
    private ReentrantReadWriteLock lock;

    public LoadInfoFromDisk(Map<String, Map<Integer, Chunk>> chunkMap){
        this.chunkMap = chunkMap;
        lock = new ReentrantReadWriteLock();
    }

    public void loadInfo(Path path){
        queue.execute(new ReadInfo(path));
    }

    private void writeInfoToMap(String filename, Chunk chunk){
        lock.lockWrite();
        try{
            if (chunkMap.containsKey(filename)){
                Map<Integer, Chunk> map = chunkMap.get(filename);
                map.put(chunk.getId(), chunk);
            } else{
                Map<Integer, Chunk> map = new HashMap<>();
                map.put(chunk.getId(), chunk);
                chunkMap.put(filename, map);
            }
        }finally {
            lock.unlockWrite();
        }
    }

    private Chunk createChunk(String path){
        if (path == null || path.length() == 0){
            System.out.println("createChunk: invalid path name");
            return null;
        }
        //path likes "./data/filename/md5-id"
        String[] strings = path.split("/");
        String filename = strings[strings.length - 2];
        String[] infos = strings[strings.length - 1].split("-");
        Chunk chunk = new Chunk(filename, infos[0], Integer.valueOf(infos[1]));
        return chunk;
    }

    public class ReadInfo implements Runnable {
        private Path directory; // the directory that this DirectoryWorker is responsible for
        ReadInfo(Path dir) {
            directory = dir;
            //logger.debug("Thread begain");
            incrementTasks();
        }
        @Override
        public void run() {
            try {
                for (Path path : Files.newDirectoryStream(directory)) {
                    if (Files.isDirectory(path)) {
                        queue.execute(new ReadInfo(path)); // add new DirectoryWorker to the work queue
                    } else {
                        Chunk chunk = createChunk(path.toString());
                        writeInfoToMap(chunk.getFilename(), chunk);
                    }
                }
            } catch (IOException e) {
                //logger.warn("Unable to calculate size for {}", directory);
                //logger.catching(Level.DEBUG, e);
                e.printStackTrace();
            }
            finally {
                decrementTasks(); // done with this task
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
        queue.shutdown();
        queue.awaitTermination();
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
