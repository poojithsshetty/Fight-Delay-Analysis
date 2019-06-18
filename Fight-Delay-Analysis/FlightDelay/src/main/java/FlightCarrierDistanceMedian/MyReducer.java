package FlightCarrierDistanceMedian;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MyReducer extends Reducer<Text, SortedMapWritable, Text,MedianStdDevTuple> {
    @Override
    protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {

        //int sum = 0;
        //int count = 0;
        MedianStdDevTuple result = new MedianStdDevTuple();
        TreeMap<Float, Integer> finalMap= new TreeMap<Float, Integer>();

        try{
            Float sum = 0f;
            Integer totalCount = 0;
            for(SortedMapWritable v:values) {
                for(Object entry: v.entrySet() ) {
                    if(entry instanceof Map.Entry) {
                        Map.Entry entry1 = (Map.Entry) entry;
                        Float mapKey = ((FloatWritable) entry1.getKey()).get();
                        Integer mapValue = ((IntWritable) entry1.getValue()).get();
                        Integer count = finalMap.get(mapKey);

                        sum = sum + mapValue * mapKey;
                        totalCount = totalCount + mapValue;


                        if (count != null) {
                            finalMap.put(mapKey, mapValue + count);
                        } else {
                            finalMap.put(mapKey, mapValue);
                        }
                    }

                }
                v.clear();
            }
            Integer middleIndex = totalCount/2;
            int prev=0;
            int current = 0;
            Float prevKey = 0f;
            Float median=0f;


            for(Map.Entry<Float,Integer> entry: finalMap.entrySet()) {
                current = entry.getValue();
                if(middleIndex > prev && middleIndex<=(current+prev)) {
                    if(totalCount%2==0) {
                        median = (entry.getKey()+prevKey)/2;

                    } else {
                        median = entry.getKey();
                    }
                   break;
                }
                prev= current+prev;
                prevKey = entry.getKey();

            }
            result.setMedian(median);

            Float mean = sum/totalCount;

            Float sumOfSquares=0.f;
            for(Map.Entry<Float,Integer> entry:finalMap.entrySet()) {
                sumOfSquares = sumOfSquares + (entry.getKey()-mean)*(entry.getKey()-mean);
            }
            Float stdDev = (float) Math.sqrt(sumOfSquares/(totalCount-1));

            result.setStdDev(stdDev);

            context.write(key,result);

        } catch(Exception e) {
            System.out.println("exception in reducer "+ e);
        }
    }
}
