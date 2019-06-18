package DelayFMedian;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       try {
           String split[]=value.toString().split(",");
          // IntWritable one = new IntWritable(1);
           String  symbol = split[0];
           if(!symbol.contains("YEAR") && split.length>20 && !split[31].equalsIgnoreCase("0") && !split[31].equalsIgnoreCase("") && !split[31].equalsIgnoreCase("")) {
               context.write(new Text(split[6]),new Text(split[31]));
           }

       } catch(Exception ex) {
           System.out.println("Exception in mapper"+ex);
       }
    }
}
