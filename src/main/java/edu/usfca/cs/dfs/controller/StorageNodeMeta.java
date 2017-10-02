package edu.usfca.cs.dfs.controller;

import edu.usfca.cs.dfs.storageNode.StorageNode;
import edu.usfca.cs.dfs.utilities.Chunk;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tuo on 22/09/17.
 * Class StorageNodeMeta records following data:
 * name: name of the StorageNode(bass01, bass02,.....)
 * numOfChunks: how many chunks stored in this node
 * isActive: the status of this node
 * chunkMap: information of all the chunks
 *          key: file's name
 *          value: chunks stored at this node from specific file(key: chunk id. value: (id, filename, md5))
 */
public class StorageNodeMeta {
    private String name;
    private int numOfChunks;
    private Map<String, Map<Integer, Chunk>> chunkMap;
    private boolean isActive;

    public StorageNodeMeta(String name, Map<String, Map<Integer, Chunk>> chunkMap){
        this.name = name;
        this.chunkMap = new HashMap<String, Map<Integer, Chunk>>(chunkMap);
        this.isActive = true;
    }

    /*
    * Calculate the number of chunks that stored at this node. We use numOfChunks to do the
    * storage balance. If controller need to choose a storage node for clients, it always the
    * storage node with least numOfChunks
    * */
    public void calNumOfChunks(){
        for (Map.Entry<String, Map<Integer, Chunk>> entry : chunkMap.entrySet())
            numOfChunks += entry.getValue().size();
    }

    /*
    * We need to increase numOfChunks when this node stored a new chunk.
    * */
    public void increaseNumOfChunks(){ numOfChunks++;}

    /*
    * If we find a chunk at this node has been broken, we need to decrease this num since we cannot
    * use this chunk anymore.
    * */
    public void decreaseNumOfChunks(){ numOfChunks--;}

    public void updateStatus(boolean b){ isActive = b;}

    public String getName(){return this.name;}
    public int getNumOfChunks(){return this.numOfChunks;}
    public Map<String, Map<Integer, Chunk>> getChunkMap(){return this.chunkMap;}
    public boolean getStatus(){return this.isActive;}
}
