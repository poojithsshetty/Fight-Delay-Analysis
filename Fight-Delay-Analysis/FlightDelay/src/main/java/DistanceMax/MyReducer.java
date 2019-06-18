package DistanceMax;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer currentMaxRateValue;
        Integer currentMinRateValue;

        Integer maxRateValue =  0;
        Integer minRateValue = Integer.MAX_VALUE;

        for (Text value : values) {
            String a[]=value.toString().split("\t");
            currentMaxRateValue = (int) Float.parseFloat(a[1].trim().replace("\"", "").trim());
            currentMinRateValue =  (int) Float.parseFloat(a[0].trim().replace("\"", "").trim());

            if (currentMaxRateValue > maxRateValue) {
                maxRateValue = currentMaxRateValue;
            }
            if (currentMinRateValue < minRateValue) {
                minRateValue = currentMinRateValue;
            }
        }
        context.write(key, new Text(minRateValue+"\t"+maxRateValue));
    }
}
