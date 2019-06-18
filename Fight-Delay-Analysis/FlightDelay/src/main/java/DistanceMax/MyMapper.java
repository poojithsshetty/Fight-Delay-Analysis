package DistanceMax;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text symbol;
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields.length > 20) {
            if (!line.contains("YEAR")) {
                symbol = new Text(fields[6].replace("\"", "").trim());
                context.write(symbol, new Text(fields[39]+"\t"+fields[39]));
            }

        }
    }
}