package FlightCarrierDistanceMedian;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, SortedMapWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       try {
           String split[]=value.toString().split(",");
           IntWritable one = new IntWritable(1);
           String  symbol = split[6];
           if(!split[0].contains("YEAR") && split.length>30 && !split[31].equalsIgnoreCase("0") && !split[31].equalsIgnoreCase("\"0\"")) {
               Float avg = Float.parseFloat(split[39]);
               SortedMapWritable map = new SortedMapWritable();
               map.put(new FloatWritable(avg),one);
               context.write(new Text(symbol.replace("\"","")),map);
           }

       } catch(Exception ex) {
           System.out.println("Exception in mapper"+ex);
       }
    }
}
