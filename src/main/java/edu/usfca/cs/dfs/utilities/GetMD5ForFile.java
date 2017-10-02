package edu.usfca.cs.dfs.utilities;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;


/*
 * Created by tuo on 20/09/17.
 */
public class GetMD5ForFile {

    private File file;

    public GetMD5ForFile(String filePath){
        this.file = new File(filePath);
    }

    public String getMD5(){
        String md5 = null;
        FileInputStream fileInputStream = null;
        try{
            fileInputStream = new FileInputStream(this.file);
            md5 = DigestUtils.md5Hex(IOUtils.toByteArray(fileInputStream));
            fileInputStream.close();
        }catch(Exception io){
            io.printStackTrace();
        }
        return md5;
    }
}
