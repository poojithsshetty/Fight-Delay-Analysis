package InverteBloomChain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String concatString = "";

       for(Text s:values) {
           if(!concatString.contains(s.toString())) {
               concatString = s.toString() + "," + concatString;
           }

       }
       context.write(key,new Text(concatString));
    }
}
