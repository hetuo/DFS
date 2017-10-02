package edu.usfca.cs.dfs.utilities;

/**
 * Created by tuo on 22/09/17.
 */
public class Chunk {
    private String filename;
    private String md5;
    private int id;

    public Chunk(String filename, String md5, int id){
        this.filename = filename;
        this.md5 = md5;
        this.id = id;
    }

    public String getFilename() { return filename;}
    public String getMd5() { return md5;}
    public int getId() { return id;}

}
