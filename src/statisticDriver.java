import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class statisticDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("/纳兰词全集计算/part-r-00000");
        Path output = new Path("/纳兰词全集结果");
        Configuration configuration = new Configuration ();
        configuration.set ("fs.defaultFS", "hdfs://192.168.43.128:9000");
        Job job = Job.getInstance (configuration);

        FileSystem fileSystem = FileSystem.get (configuration);
        if(fileSystem.exists (output)){
            fileSystem.delete (output,true);
        }
        job.setJarByClass (statisticDriver.class);
        job.setMapperClass (statisticMapper.class);
        job.setReducerClass (statisticReducer.class);
//        job.setNumReduceTasks (1);

        job.setMapOutputKeyClass (NullWritable.class);
        job.setMapOutputValueClass (MapWritable.class);
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (LongWritable.class);

        FileInputFormat.setInputPaths (job,input);
        FileOutputFormat.setOutputPath (job,output);

        if(job.waitForCompletion (true)){
            System.out.println ("程序执行完毕");
        }

    }
}
