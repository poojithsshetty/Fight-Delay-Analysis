package CountersDistanceGroup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class MyMapper extends Mapper<LongWritable, Text, NullWritable,NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String distances[]= {"1","2","3","4","5","6","7", "8","9","10","11","12"};
        HashSet<String> distanceHashSet= new HashSet<String>(Arrays.asList(distances));

        String input = value.toString();
        String splitString[] = input.split(",");
        if(splitString.length > 39 && !value.toString().contains("YEAR")) {
            String distanceGroup = splitString[40].replace("\"","").trim();
            //  System.out.print(month);

            if (distanceHashSet.contains(distanceGroup) && splitString[40] != "grade") {
                // System.out.print(month);
                context.getCounter("DistanceCounter", distanceGroup).increment(1);
            } else {
                context.getCounter("DistanceCounter", "unknown").increment(1);

            }
        }


    }
}
