package Distinct;

import SecSortPartitionerWritable.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
//        Job job = Job.getInstance();
//
//        job.setJarByClass(Driver.class);
//
//        job.setGroupingComparatorClass(GroupComparator.class);
//        job.setSortComparatorClass(SecodarySortComparator.class);
//        job.setPartitionerClass(KeyPartition.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        Path outDir = new Path(args[1]);
//        FileOutputFormat.setOutputPath(job, outDir);
//
//        job.setMapperClass(MyMapper.class);
//        job.setReducerClass(MyReducer.class);
//
//        job.setNumReduceTasks(1);
//
//        job.setOutputKeyClass(CompositeKeyWritable.class);
//        job.setOutputValueClass(NullWritable.class);
//
//        FileSystem fs = FileSystem.get(job.getConfiguration());
//        if(fs.exists(outDir)) {
//        	fs.delete(outDir, true);
//        }
//
//        Boolean a=job.waitForCompletion(true);
//        if(a==true)
//        {
            Job job2 = Job.getInstance();

            job2.setJarByClass(InverteBloomChain.Driver.class);


            FileInputFormat.addInputPath(job2, new Path(args[0]));
            Path outDir1 = new Path(args[1]);
            FileOutputFormat.setOutputPath(job2, outDir1);

            job2.setMapperClass(DistinctMapper.class);
            job2.setReducerClass(DistinctReducer.class);
            job2.setCombinerClass(DistinctReducer.class);

            job2.setOutputFormatClass(TextOutputFormat.class);
            job2.setMapOutputValueClass(NullWritable.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setInputFormatClass(NLineInputFormat.class);
            NLineInputFormat.setNumLinesPerSplit(job2, 10000);
            job2.setNumReduceTasks(1);

            job2.waitForCompletion(true);
   //     }
    }
}
