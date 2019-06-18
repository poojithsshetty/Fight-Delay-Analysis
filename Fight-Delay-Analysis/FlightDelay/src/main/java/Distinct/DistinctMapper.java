package Distinct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       try {
           String input = value.toString();

           String splitString[] = input.split(",");
           if(splitString.length>10 && !input.contains("YEAR")) {
               String origin = splitString[6].replace("\"","").trim();

               context.write(new Text(origin), NullWritable.get());
           }
       } catch(Exception e) {
           System.out.println(e);
       }
    }
}
