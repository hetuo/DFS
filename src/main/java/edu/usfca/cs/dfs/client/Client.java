package edu.usfca.cs.dfs.client;

import edu.usfca.cs.dfs.concurrent.WorkQueue;
import org.apache.commons.codec.digest.DigestUtils;
import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.utilities.GetMD5ForFile;
import edu.usfca.cs.dfs.utilities.StorageMessages;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.util.*;

public class Client {

    public static final int BUFFER_SIZE = 1024 * 1024;
    private Map<String, FileInfo> fileInfoMap;
    private Socket clientForController;
    private String filename;
    private int numOfChunks;
    private List<StorageMessages.StoreChunk> chunkList;
    private LinkedList<List<String>> storageNodeList;
    private WorkQueue workQueue = new WorkQueue(10);

    public Client(String filename){
        try {
            //clientForController = new Socket("localhost", 8000);
            this.filename = filename;
            chunkList = new LinkedList<>();
            storageNodeList = new LinkedList<>();
        }catch(Exception e){
            e.printStackTrace();
        }
        fileInfoMap = new HashMap<>();
    }

    public void recordToMap(String name, FileInfo fileInfo){
        fileInfoMap.put(name, fileInfo);
    }

    private void recordFileInfo(String filename){
        if (filename == null || filename.length() == 0){
            System.out.println("createChunks: Invalid filename");
        }
        try{
            File file = new File(filename);
            long size = file.length();
            GetMD5ForFile getMD5ForFile = new GetMD5ForFile(filename);
            String md5 = getMD5ForFile.getMD5();
            FileInfo fileInfo = new FileInfo(filename, size, md5);
            fileInfoMap.put(filename, fileInfo);
            System.out.println("The information of " + filename + " has been recorded!");
        } catch (Exception io){
            io.printStackTrace();
        }
    }

    private void createFileByChunks(List<StorageMessages.StoreChunk> chunkList, String filename){
        if (chunkList == null || chunkList.size() == 0){
            System.out.println("createFileByChunks: invalid chunkList");
            return;
        }
        FileOutputStream fileOutputStream = null;
        long size = fileInfoMap.get(filename).getFileSize();
        String md5 = fileInfoMap.get(filename).getMd5();
        try{
            fileOutputStream = new FileOutputStream("new" + filename);
            for (int i = 0; i <= chunkList.size() - 2; i++){
                size -= BUFFER_SIZE;
                StorageMessages.StoreChunk chunk = chunkList.get(i);
                byte[] data = chunk.getData().toByteArray();
                fileOutputStream.write(data);
            }
            byte[] data = chunkList.get(chunkList.size() - 1).getData().toByteArray();
            fileOutputStream.write(data, 0, (int)size);
        }catch (Exception io){
            io.printStackTrace();
        }finally {
            try{
                if (fileOutputStream != null){
                    fileOutputStream.close();
                    GetMD5ForFile getMD5ForFile = new GetMD5ForFile("new" + filename);
                    if (getMD5ForFile.getMD5().equals(md5))
                        System.out.println("createFileByChunks: retrieve " + filename + " successfully!");
                    else
                        System.out.println("createFileByChunks: retrieve " + filename + " unsuccessfully!");
                }
            }catch(Exception io){
                io.printStackTrace();
            }
        }

    }

    /**
     * Split the file to create the chunk list before we send store request to Controller.
     * Update variable numOfChunks and chunkList. Send numOfChunks to Controller to get the
     * StorageNode list. Send the chunks in the Queue chunkList to those nodes by Thread Pool
     */
    private void createChunks(String filename){
        if (filename == null || filename.length() == 0){
            System.out.println("createChunks: Invalid filename");
            return ;
        }
        //List<StorageMessages.StoreChunk> chunkList = new ArrayList<>();
        InputStream is = null;
        try{
            byte[] buffer = new byte[BUFFER_SIZE];
            is = new FileInputStream(filename);
            for (int i = 0; is.read(buffer) != -1; i++){
                String md5 = DigestUtils.md5Hex(buffer);
                ByteString data = ByteString.copyFrom(buffer);
                StorageMessages.StoreChunk storeChunkMsg
                        = StorageMessages.StoreChunk.newBuilder()
                        .setFileName(filename)
                        .setChunkId(i)
                        .setMd5(md5)
                        .setData(data)
                        .build();
                chunkList.add(storeChunkMsg);
            }
            this.numOfChunks = chunkList.size();
        } catch(Exception io){
            io.printStackTrace();
        } finally {
            try{
                if (is != null)
                    is.close();
            } catch(Exception io){
                io.printStackTrace();
            }
        }
    }



    public static void main(String[] args) throws Exception {
        if (args.length != 2 || args[1] == ""){
            System.out.println("Client usage: ./client store(or retrieve) filename");
            return;
        }
        if (args[0].equals("store")){
	    StoreFile test = new StoreFile();
            test.storeFile(args[1]);
            System.out.println("Done");
	    }
        else if (args[0].equals("retrieve")){
            RetrieveFile retrieve = new RetrieveFile(args[1]);
            retrieve.retrieveFile(args[1]);
            System.out.println("Done");
        }
        else if (args[0].equals("list")){
            GetFileList getList = new GetFileList();
            getList.getFileList();
            System.out.println("Done");
        }
        else{
            System.out.println("Client usage: ./client store(or retrieve) filename");
            return;
        }
	    return;
    }

}
