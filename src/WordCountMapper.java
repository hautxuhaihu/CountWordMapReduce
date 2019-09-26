import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.*;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        byte[] bt = line.getBytes();
        InputStream ip = new ByteArrayInputStream(bt);
        Reader read = new InputStreamReader(ip);
        IKSegmenter iks = new IKSegmenter(read, true);
        Lexeme t;
        while ((t = iks.next()) != null) {
            context.write(new Text(t.getLexemeText()),new LongWritable(1));
        }
    }
}
