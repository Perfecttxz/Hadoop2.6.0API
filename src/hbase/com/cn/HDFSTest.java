package hbase.com.cn;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
 
public class HDFSTest {
     
     
    public void WriteFile(String hdfs,String note) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setBoolean("dfs.support.append", true);  
        FileSystem fs = FileSystem.get(URI.create(hdfs),conf);
        FSDataOutputStream hdfsOutStream = fs.create(new Path(hdfs));
        hdfsOutStream.writeChars(note);
        hdfsOutStream.close();
        fs.rename(new Path(hdfs), new Path("hdfs://yingji:8020/abc/hi.txt"));
        fs.close();     
    }
     
    public void ReadFile(String hdfs) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfs),conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));
         
        byte[] ioBuffer = new byte[1024];
        int readLen = hdfsInStream.read(ioBuffer);
        while(readLen!=-1)
        {
            System.out.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        hdfsInStream.close();
        fs.close(); 
    }
         
    public static void main(String[] args) throws IOException {     
        String hdfs = "hdfs://yingji:8020/abc/hello.txt";
        HDFSTest t = new HDFSTest();        
        t.WriteFile(hdfs,"123");
        t.ReadFile(hdfs);
        
      }
}