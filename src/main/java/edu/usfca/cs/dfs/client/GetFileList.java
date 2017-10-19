package edu.usfca.cs.dfs.client;

import edu.usfca.cs.dfs.utilities.StorageMessages;

import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by tuo on 19/10/17.
 */
public class GetFileList {

    public void getFileList(){
        Socket client = null;
        try{
            client = new Socket("bass01.cs.usfca.edu", 21000);
            StorageMessages.GetFileList message = StorageMessages.GetFileList.newBuilder().setTest(0).build();
            StorageMessages.StorageMessageWrapper msgWrapper =
                    StorageMessages.StorageMessageWrapper.newBuilder()
                    .setGetFileListMsg(message).build();
            msgWrapper.writeDelimitedTo(client.getOutputStream());
            msgWrapper = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(client.getInputStream());
            if (msgWrapper.hasGetAllDataResponseMsg()){

                StorageMessages.GetFileListResponse response = msgWrapper.getGetFileListResponseMsg();
                List<String> list = response.getFileListList();
                for (String filename : list)
                    System.out.println(filename);
            }
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            if (client != null){
                try{
                    client.close();
                }catch (Exception e){e.printStackTrace();}
            }
        }

    }
}
