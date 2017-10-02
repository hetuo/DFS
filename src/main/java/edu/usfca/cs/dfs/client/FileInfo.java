package edu.usfca.cs.dfs.client;

/**
 * Created by tuo on 20/09/17.
 */
public class FileInfo {
    private String filename;
    private long fileSize;
    private String md5;

    public FileInfo(String filename, long fileSize, String md5){
        this.filename = filename;
        this.fileSize = fileSize;
        this.md5 = md5;
    }

    public String getFilename(){
        return this.filename;
    }

    public String getMd5(){
        return this.md5;
    }

    public long getFileSize(){
        return this.fileSize;
    }

}
