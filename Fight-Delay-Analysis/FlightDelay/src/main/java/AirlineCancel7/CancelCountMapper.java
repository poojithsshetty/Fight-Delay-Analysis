package AirlineCancel7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CancelCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String t= "";
        String[] tokens = line.split(",");
      //  for(String s : tokens) {
        if(!tokens[0].contains("YEAR") && tokens.length >33 && !tokens[32].equalsIgnoreCase("0")
                && !tokens[33].equalsIgnoreCase(" ") && !tokens[33].replace("\"","").trim().equalsIgnoreCase("")) {

            context.write(new Text(tokens[6].replace("\"","").trim()+"\t"+tokens[33].replace("\"","").trim()+"\t"+tokens[2]), new Text(1+""));
        }
        //}
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
    }

    @Override
    public void run(Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.run(context);
    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(context);
    }

}
