import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

public class statisticMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] array = value.toString ().split ("\t");
        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new Text(array[0]),new LongWritable(Integer.valueOf(array[1])));
//        for(int i=0;i<array.length;i++){
//            mapWritable.put(new Text(array[i]),new LongWritable(Integer.valueOf(array[i+1])));
//            i = i+1;
//        }
        context.write (NullWritable.get(),mapWritable);
    }
}
