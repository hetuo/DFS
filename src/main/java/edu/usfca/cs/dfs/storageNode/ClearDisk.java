package edu.usfca.cs.dfs.storageNode;

import edu.usfca.cs.dfs.concurrent.ReentrantReadWriteLock;
import edu.usfca.cs.dfs.concurrent.WorkQueue;
import edu.usfca.cs.dfs.utilities.Chunk;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tuo on 05/10/17.
 */
public class ClearDisk {
    private WorkQueue queue = new WorkQueue();
    private volatile int numTasks = 0;

    public void deleteDirectory(Path path){
        try {
            File f = new File(path.toString());
            if (!f.exists())
                return;
            queue.execute(new Delete(path));
            shutdown();
            for (Path folder : Files.newDirectoryStream(path))
                Files.deleteIfExists(folder);
            Files.deleteIfExists(path);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public class Delete implements Runnable {
        private Path directory; // the directory that this DirectoryWorker is responsible for
        Delete(Path dir) {
            directory = dir;
            incrementTasks();
        }
        @Override
        public void run() {
            try {
                for (Path path : Files.newDirectoryStream(directory)) {
                    if (Files.isDirectory(path)) {
                        queue.execute(new Delete(path)); // add new DirectoryWorker to the work queue
                    } else {
                        Files.deleteIfExists(path);
                    }
                }
            } catch (IOException e) {
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
