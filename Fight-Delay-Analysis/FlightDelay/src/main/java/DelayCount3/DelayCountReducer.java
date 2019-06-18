package DelayCount3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DelayCountReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {

        int sum = 0;
        int count=0;
        for (Text value : values) {
            String a[]=value.toString().split(" ");
            sum += Float.parseFloat(a[0]);
            count+=Integer.parseInt(a[1]);
        }

        context.write(key, new Text(sum+" "+count));
    }

    @Override
    public void run(Context arg0)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.run(arg0);
    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(context);
    }

}
