package InverteBloomChain;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = Job.getInstance();

        job.setJarByClass(Driver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outDir = new Path(args[1]+"_1");
        FileOutputFormat.setOutputPath(job, outDir);

        job.setMapperClass(FilterMapper.class);
      //  job.setReducerClass(MyReducer.class);
       // job.setCombinerClass(MyReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

      //  job.setNumReduceTasks(1);


        Boolean a=job.waitForCompletion(true);

        if(a==true)
        {
            Job job2 = Job.getInstance();

            job2.setJarByClass(Driver.class);

            // job2.setGroupingComparatorClass(GroupComparator.class);
            // job2.setSortComparatorClass(SecodarySortComparator.class);
            //job2.setPartitionerClass(KeyPartition.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]+"_1"));
            Path outDir1 = new Path(args[1]);
            FileOutputFormat.setOutputPath(job2, outDir1);

            job2.setMapperClass(MyMapper.class);
            job2.setReducerClass(MyReducer.class);
            job2.setCombinerClass(MyReducer.class);

            job2.setOutputFormatClass(TextOutputFormat.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setInputFormatClass(TextInputFormat.class);

            job2.setNumReduceTasks(1);

            // job2.setOutputKeyClass(CompositeKeyWritable.class);
            // job2.setOutputValueClass(NullWritable.class);


            job2.waitForCompletion(true);
        }
    }
}
