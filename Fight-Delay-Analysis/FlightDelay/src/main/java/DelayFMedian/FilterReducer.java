package DelayFMedian;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class FilterReducer extends Reducer<Text, Text, Text,Text> {
    ArrayList<Integer> array= new ArrayList<Integer>();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Integer median=0;
        //int sum = 0;
        //int count = 0;

//Integer temp= (int) Float.parseFloat(key.toString());
        try {
            for (Text value:values) {

                Integer temp = (int) Float.parseFloat(value.toString());
                if (temp != 0){
                    array.add(temp);
                }
                //context.write(new Text(key), NullWritable.get());
            }
            Collections.sort(array);
            int len=array.size();
            if(array.size()%2==0){
               // median= array.get((int) len/2 - 1)+array.get((int) len/2);
                median= (array.get((int) len/2 - 1)+array.get((int) len/2)) /2;
            }
            else{
                median= array.get(len/2);
            }

            context.write(key,new Text(median+""));

        }
        catch (Exception e){
            System.out.println(key+e.getMessage());
        }
    }
}
