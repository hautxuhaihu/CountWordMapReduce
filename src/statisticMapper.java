import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

public class statisticMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable>{
    /**
     * 在WordCount的结果上继续操作，统计出现最多的三个词的频率
     * @param key  读入文件
     * @param value 按行读入的内容在value中
     * @param context 上下文，将分析好的数据传给reducer
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] array = value.toString ().split ("\t");
        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new Text(array[0]),new LongWritable(Integer.valueOf(array[1])));
        context.write (NullWritable.get(),mapWritable);
    }
}
