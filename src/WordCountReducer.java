import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    /**
     * 计算每个词的数量
     * @param key mapper传来数据的key值，内容是词的名称
     * @param values 每个词对应的list，list中的元素都是1，1的数量代表词出现的频率
     * @param context 上下文，可以写文件
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;//定义和
        for(LongWritable item:values){
            sum=sum+item.get ();//统计数字1的个数
        }
        context.write (new Text (key),new LongWritable (sum));
    }
}
