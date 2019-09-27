import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSAPI {
    /*
    本api用于向hdfs上上传元数据文件
     */
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.43.128:9000");//hadoop所在的机器的地址
        configuration.set("dfs.replication","1");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/data");//在hadoop创建新的文件夹
        if(!fileSystem.isDirectory(path)){
            fileSystem.mkdirs(path);
            System.out.println("创建成功");
        }
        fileSystem.copyFromLocalFile(new Path("D:\\纳兰词全集.txt"),path);//将本地文件上传
    }
}
