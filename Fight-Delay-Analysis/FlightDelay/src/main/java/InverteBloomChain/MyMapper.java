package InverteBloomChain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       try {
           String input = value.toString();

           String splitString[] = input.split(",");
           String origin = splitString[10];

           context.write(new Text(origin.replace("\"","").trim()), new Text(splitString[6].replace("\"","").trim()));
       } catch(Exception e) {
           System.out.println(e);
       }
    }
}
