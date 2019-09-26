import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class statisticReducer extends Reducer<NullWritable, MapWritable,NullWritable,LongWritable> {
    @Override
    protected void reduce(NullWritable nullWritable, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable mapWritable = new MapWritable();
        for(MapWritable item:values){
            Set<Writable> set = item.keySet();
            for(Writable key:set){
                mapWritable.put(key,item.get(key));
            }
        }
        ArrayList <Integer> arrayList = new ArrayList();
        arrayList.add(0,0);
        arrayList.add(1,0);
        arrayList.add(2,0);

        Set<Writable> set = mapWritable.keySet();
        for(Writable writable:set){
            int num = Integer.parseInt(mapWritable.get(writable).toString());
            if(num>arrayList.get(0)){
                arrayList.set(2,arrayList.get(1));
                arrayList.set(1,arrayList.get(0));
                arrayList.set(0,num);
            }
        }

//        for(MapWritable item:values){
//            Set<Writable> set = item.keySet();
//            for(Writable writable:set){
//                int num = Integer.parseInt(item.get(writable).toString());
//               if(num>arrayList.get(0)){
//                   arrayList.set(2,arrayList.get(1));
//                   arrayList.set(1,arrayList.get(0));
//                   arrayList.set(0,num);
//               }
//            }
//        }
        for(Integer integer:arrayList){
            context.write (NullWritable.get(),new LongWritable(integer));
        }
    }
}
