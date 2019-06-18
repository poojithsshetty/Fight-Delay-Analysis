package FlightCarrierDistanceMedian;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

public class myCombiner extends Reducer<Text, SortedMapWritable, Text, SortedMapWritable> {
    @Override
    protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
       // int sum=0;
        SortedMapWritable map = new SortedMapWritable();
        for(SortedMapWritable v:values) {
            for(Object entry : v.entrySet() ) {
                if(entry instanceof Map.Entry) {
                    Map.Entry entry1 = (Map.Entry) entry;
                    IntWritable count = (IntWritable) map.get(entry1.getKey());
                    FloatWritable mapKey = ((FloatWritable) entry1.getKey());
                    Integer mapValue = ((IntWritable) entry1.getValue()).get();
                    if (count != null) {
                        map.put(mapKey, new IntWritable(count.get() + ((IntWritable) entry1.getValue()).get()));
                    } else {
                        map.put(mapKey, new IntWritable(((IntWritable) entry1.getValue()).get()));
                    }
                }
            }
            v.clear();
        }
        context.write(key,map);

    }
}

