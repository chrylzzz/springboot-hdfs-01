package com.lnsoft.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by 迟钰林 on 2018/11/29/0029.
 * <p>
 * java操作hdfs系统
 */

public class HdfsTest {

    static FileSystem  fs=null;
    static{
        //获取配置文件
        Configuration conf = new Configuration();
        try {
            fs=FileSystem.get(new URI("hdfs:192.168.216.111:9000"),conf,"root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws Exception {
        //path是hdfs的地址
        readFileToConsole("/目录/if.sh");
        readFileToLocal("/目录/if.sh");
        //上传到hdfs
        copyFormToLocal();
    }
    //读取hdfs文件系统中的文件到控制台
    public static void readFileToConsole(String path) throws Exception {
        //获取配置文件
        Configuration conf = new Configuration();
        //配置
        //xml配置的name
        conf.set("fs.defaultFs", "hdfs:192.168.216.111:9000");
        //获取hdfs的文件体统的操作对象
        fs = FileSystem.get(conf);
        //对具体文件操作,path是hdfs下的
        FSDataInputStream fis = fs.open(new Path(path));
        //四个参数：in，out，hdfs 2 默认为4096，是否关闭
        IOUtils.copyBytes(fis, System.out, 4096, true);

    }
    //读取hdfs文件系统中的文件到本地
    public static void readFileToLocal(String path) throws IOException {
        FSDataInputStream fis=null;
        OutputStream out=null;
        try {
            //获取配置文件
            Configuration conf = new Configuration();
            //配置
            //xml配置的name
            //conf.set("fs.defaultFs", "hdfs:192.168.216.111:9000");
            FileSystem  fs= FileSystem.get(new URI("hdfs:192.168.216.111:9000"),conf,"root");
            //对具体文件操作,path是hdfs下的
            fis = fs.open(new Path(path));
            //输出流到本地
            out=new FileOutputStream(new File("G:\\Java相关工具\\hadoop\\test01.txt"));
            //四个参数：in，out，hdfs 2 默认为4096，是否关闭
            IOUtils.copyBytes(fis, out, 4096, true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fis.close();
            out.close();
        }

    }
    //将windows上传到hdfs文件系统中
    public static void copyFormToLocal() throws IOException {
        //1，本地,2 hdfs下的
        fs.copyFromLocalFile(new Path("G:\\Java相关工具\\hadoop\\test01.txt"),new Path("/test/213"));
        System.out.println("finished...");
    }
}
