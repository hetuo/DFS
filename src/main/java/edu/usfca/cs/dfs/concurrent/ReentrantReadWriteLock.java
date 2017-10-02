package edu.usfca.cs.dfs.concurrent;

/**
 * Created by tuo on 22/09/17.
 */
import java.util.HashMap;
import java.util.Map;

/**
 * A reentrant read/write lock that allows:
 * 1) Multiple readers (when there is no writer).
 * 2) One writer (when nobody else is writing or reading).
 * 3) A writer is allowed to acquire a read lock while holding the write lock.
 * The assignment is based on the assignment of Prof. Rollins (original author).
 */
public class ReentrantReadWriteLock {

    // TODO: Add instance variables : you need to keep track of the read lock holders and the write lock holders.
    // We should be able to find the number of read locks and the number of write locks
    // a thread with the given threadId is holding
    private Map<Long, Integer>readerMap;
    private Map<Long, Integer>writerMap;
    private volatile int readers = 0;
    private volatile int writers = 0;

    /**
     * Constructor for ReentrantReadWriteLock
     */
    public ReentrantReadWriteLock() {
        // FILL IN CODE
        readerMap = new HashMap<Long, Integer>();
        writerMap = new HashMap<Long, Integer>();

    }

    /**
     * Returns true if the current thread holds a read lock.
     *
     * @return
     * 		true  current thread hold the read lock
     * 		false current thread don't hold the read lock
     */
    public synchronized boolean isReadLockHeldByCurrentThread() {

        // FILL IN CODE
        long currentThreadId = Thread.currentThread().getId();
        if (null == readerMap.get(currentThreadId) || 0 == readerMap.get(currentThreadId))
            return false; // don't forget to change it
        else
            return true;
    }

    /**
     * Returns true if the current thread holds a write lock.
     *
     * @return
     * 		true  current thread hold the write lock
     * 		false current thread don't hold the write lock
     */
    public synchronized boolean isWriteLockHeldByCurrentThread() {
        // FILL IN CODE
        long currentThreadId = Thread.currentThread().getId();
        if (null == writerMap.get(currentThreadId) || 0 == writerMap.get(currentThreadId))
            return false; // don't forget to change it
        else
            return true;
    }

    /**
     * Non-blocking method that tries to acquire the read lock. Returns true
     * if successful.
     *
     * @return
     * 		true get the read lock successfully
     * 		false failde to get the read lock
     */
    public synchronized boolean tryAcquiringReadLock() {
        // FILL IN CODE
        long currentThreadId = Thread.currentThread().getId();
        //If nobody holds the write lock or the holder of write lock is current thread right now,
        //we can acquire the write lock immediately. If not, we just return false
        if (writers == 0 || (null != writerMap.get(currentThreadId) && writers == writerMap.get(currentThreadId)))
        {
            if (readerMap.get(currentThreadId) == null)
                readerMap.put(currentThreadId, 1);
            else
            {
                int num = readerMap.get(currentThreadId);
                readerMap.put(currentThreadId, num + 1);
            }
            readers++;
            return true;
        }
        else
            return false; // don't forget to change it
    }

    /**
     * Non-blocking method that tries to acquire the write lock. Returns true
     * if successful.
     *
     * @return
     * 	 	true get the write lock successfully
     * 		false failed to get the write lock
     */
    public synchronized boolean tryAcquiringWriteLock() {
        // FILL IN CODE
        long currentThreadId = Thread.currentThread().getId();
        //If nobody holds the write lock and read lock or the holder of write lock is current thread right now,
        //we can acquire the write lock immediately. If not, we just return false
        if ((writers == 0 || (null != writerMap.get(currentThreadId) && writers == writerMap.get(currentThreadId))) && readers == 0)
        {

            if (writerMap.get(currentThreadId) == null)
                writerMap.put(currentThreadId, 1);
            else
            {
                int num = writerMap.get(currentThreadId);
                writerMap.put(currentThreadId, num + 1);
            }
            writers++;
            return true;
        }
        return false; // don't forget to change it
    }

    /**
     * Blocking method - calls tryAcquiringReadLock and returns only when the read lock has been
     * acquired, otherwise waits.
     *
     * @throws InterruptedException
     */
    public synchronized void lockRead() {
        // FILL IN CODE
        while (false == tryAcquiringReadLock())
        {
            try {
                wait();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * Releases the read lock held by the current thread.
     */
    public synchronized void unlockRead() {
        // FILL IN CODE
        long currentThreadId = Thread.currentThread().getId();
        //If current thread didn't hold the read clock, this function would return immediately
        if (null == readerMap.get(currentThreadId) || 0 == readerMap.get(currentThreadId)){
            System.out.println(currentThreadId + ": This thread didn't hold reader lock!");
            return;
        }
        int num = readerMap.get(currentThreadId);
        readerMap.put(currentThreadId, num - 1);
        readers--;
        notifyAll();
    }

    /**
     * Blocking method that calls tryAcquiringWriteLock and returns only when the write lock has been
     * acquired, otherwise waits.
     *
     * @throws InterruptedException
     */
    public synchronized void lockWrite() {
        // FILL IN CODE
        while (false == tryAcquiringWriteLock())
        {
            try {
                wait();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * Releases the write lock held by the current thread.
     */

    public synchronized void unlockWrite() {
        // FILL IN CODE
        long currentThreadId = Thread.currentThread().getId();
        //If current thread didn't hold the write lock, this function would return immediately
        if (null == writerMap.get(currentThreadId) || 0 == writerMap.get(currentThreadId)){
            System.out.println(currentThreadId + ": This thread didn't hold writer lock!");
            return;
        }
        int num = writerMap.get(currentThreadId);
        writerMap.put(currentThreadId, num - 1);
        writers--;
        notifyAll();
    }
}
