package DelayCount3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String t= "";
        String[] tokens = line.split(",");
        Integer delay=0;
      //  for(String s : tokens) {
        if(!tokens[0].contains("YEAR") && tokens.length >45 && !tokens[41].equalsIgnoreCase("")
                && !tokens[41].equalsIgnoreCase(" ") && !tokens[41].equalsIgnoreCase("NA")) {
if(Float.parseFloat(tokens[41]) >0 ){
t="CARRIER_DELAY";
delay=(int)Float.parseFloat(tokens[41]);
}
else if(Float.parseFloat(tokens[42]) >0 ){
    t="WEATHER_DELAY";
    delay=(int)Float.parseFloat(tokens[42]);
            }
else if(Float.parseFloat(tokens[43]) >0 ){
    t="NAS_DELAY";
    delay=(int)Float.parseFloat(tokens[43]);
}
else if (Float.parseFloat(tokens[44]) >0 ){
    t="SECURITY_DELAY";
    delay=(int)Float.parseFloat(tokens[44]);
}
else{
    t="LATE_AIRCRAFT_DELAY";
    delay=(int)Float.parseFloat(tokens[45]);
}
            context.write(new Text(t+" "+tokens[2]), new Text(delay.toString()+" "+1));
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
