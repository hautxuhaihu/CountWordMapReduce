import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class statisticReducer extends Reducer<NullWritable, MapWritable,Text,LongWritable> {
    @Override
    protected void reduce(NullWritable nullWritable, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable mapWritable = new MapWritable();
        for(MapWritable item:values){
            Set<Writable> set = item.keySet();
            for(Writable key:set){
                mapWritable.put(key,item.get(key));
            }
        }

        HashMap<String,Integer> result = new HashMap<>();
        String firstName = "first";
        String secondName = "second";
        String thirdName = "third";
        result.put(firstName,0);
        result.put(secondName,0);
        result.put(thirdName,0);

        Set<Writable> set = mapWritable.keySet();
        for(Writable writable:set){
            int num = Integer.parseInt(mapWritable.get(writable).toString());
            if(num>result.get(firstName)){
                result.remove(thirdName);//最小的去掉
                thirdName = secondName;
                secondName = firstName;
                firstName = writable.toString();
                result.put(firstName,num);//加入新的最大的
            }else if(num>result.get(secondName)){
                result.remove(thirdName);//最小的去掉
                thirdName = secondName;
                secondName = writable.toString();
                result.put(secondName,num);//加入新的第二大的
            }else if(num>result.get(thirdName)){
                result.remove(thirdName);//最小的去掉
                thirdName = writable.toString();
                result.put(thirdName,num);//加入新的第三大的
            }
        }
        context.write (new Text(firstName+":"),new LongWritable(result.get(firstName)));
        context.write (new Text(secondName+":"),new LongWritable(result.get(secondName)));
        context.write (new Text(thirdName+":"),new LongWritable(result.get(thirdName)));
    }
}
