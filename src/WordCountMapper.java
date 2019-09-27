import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.*;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    /**
     * mapper是按行从hdfs中的文件中读取数据，所以前两个参数一般不变
     * @param key 从文件中读入的数据的key
     * @param value 从文件中读入的数据的value，文件中的数据都在value中
     * @param context 上下文，用来和reduce通讯，将mapper分析好的数据通过上下文传到reducer中用于计算
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); //把每一行数据转化为字符串
        byte[] bt = line.getBytes();
        InputStream ip = new ByteArrayInputStream(bt);
        Reader read = new InputStreamReader(ip);
        IKSegmenter iks = new IKSegmenter(read, true);//调用分词api分词，api在jar包中，需要引入IKAnalyzer2012.jar
        Lexeme t;
        while ((t = iks.next()) != null) {
            context.write(new Text(t.getLexemeText()),new LongWritable(1));
        }
    }
}
