package edu.usfca.cs.dfs.client;

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

    public static final int BUFFER_SIZE = 4;
    private Map<String, FileInfo> fileInfoMap;
    private Socket clientForController;

    public Client(){
        try {
            clientForController = new Socket("localhost", 8000);
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

    private List<StorageMessages.StoreChunk> createChunks(String filename){
        if (filename == null || filename.length() == 0){
            System.out.println("createChunks: Invalid filename");
            return null;
        }
        List<StorageMessages.StoreChunk> chunkList = new ArrayList<>();
        InputStream is = null;
        try{
            byte[] buffer = new byte[BUFFER_SIZE];
            is = new FileInputStream(filename);
            for (int i = 0; is.read(buffer) != -1; i++){
                ByteString data = ByteString.copyFrom(buffer);
                StorageMessages.StoreChunk storeChunkMsg
                        = StorageMessages.StoreChunk.newBuilder()
                        .setFileName(filename)
                        .setChunkId(i)
                        .setData(data)
                        .build();
                chunkList.add(storeChunkMsg);
            }
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
        return chunkList;
    }

    public void storeFile(String filename, Socket socket){
        if (filename == null || filename.length() == 0){
            System.out.println("storeFile: invalid filename");
            return;
        }
        recordFileInfo(filename);
        List<StorageMessages.StoreChunk> chunks = createChunks(filename);
        createFileByChunks(chunks, filename);
    }

    public void retrieveFile(String filename, Socket socket){

    }





    public static void main(String[] args) throws Exception {

        Client client = new Client();
//        Socket socket = new Socket("localhost", 9999);
        Socket socket = null;
        /*if (args.length != 3 || args[2] == ""){
            System.out.println("Client usage: ./client store(or retrieve) filename");
            return;
        }
        if (args[1].equals("store"))
            client.storeFile(args[2], socket);
        else if (args[1].equals("retrieve"))
            client.retrieveFile(args[2], socket);
        else{
            System.out.println("Client usage: ./client store(or retrieve) filename");
            return;
        }*/


        System.out.println(System.getProperty("user.dir"));
        socket = new Socket("localhost", 8000);
        ByteString data = ByteString.copyFromUtf8("Hello World!");
        StorageMessages.StoreChunk storeChunkMsg
                = StorageMessages.StoreChunk.newBuilder()
                .setFileName("my_file.txt")
                .setChunkId(3)
                .setData(data)
                .build();
        storeChunkMsg.toByteArray();
        StorageMessages.StorageMessageWrapper msgWrapper =
                StorageMessages.StorageMessageWrapper.newBuilder()
                        .setStoreChunkMsg(storeChunkMsg)
                        .build();

        msgWrapper.writeDelimitedTo(socket.getOutputStream());
        System.out.println("Client: already get the input stream");
        msgWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                socket.getInputStream());
        if (msgWrapper.hasStoreChunkResponseMSg()){
            StorageMessages.StoreChunkResponse message = msgWrapper.getStoreChunkResponseMSg();
            List<String> list = message.getNodeListList();
            for (String str : list)
                System.out.println("Client: " + str);
        }
        socket.close();
    }

}
