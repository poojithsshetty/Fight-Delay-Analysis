package Join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");
        Text t1 = new Text(fields[0].replace("\"","").trim());
        String val = "R"+fields[0].replace("\"","").trim();
        Text t2 = new Text(val);
        context.write(t1,t2);
    }
}