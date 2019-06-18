package CountersDistanceGroup;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = Job.getInstance();

        job.setJarByClass(Driver.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outDir);

        job.setMapperClass(MyMapper.class);




        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);



        int code = job.waitForCompletion(true)? 1 : 0;
        if (code==1) {
            System.out.print("the counter output:\n");
            for(Counter counter: job.getCounters().getGroup("DistanceCounter")) {
                //
                System.out.println(counter.getDisplayName()+":\t"+counter.getValue());
                // System.out.println("the counter values is "+counter.getValue());
            }

        }
    }
}
