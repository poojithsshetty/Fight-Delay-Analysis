package Join;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinPattern {

    public static void main(String[] args){

        try {
            Job job = Job.getInstance();
            job.setJarByClass(JoinPattern.class);

            job.getConfiguration().set("type",args[3]);

            MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, DataMapper.class);
            MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, AirlineMapper.class);

            //Reducer
            job.setReducerClass(JoinReducer.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //Number of Reducers
            job.setNumReduceTasks(1);

            //Specify Key value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //Specify Output Path
            Path output = new Path(args[2]);
            FileOutputFormat.setOutputPath(job, output);

            FileSystem fs = FileSystem.get(job.getConfiguration());
            if(fs.exists(output)) {
                fs.delete(output, true);
            }

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(Exception ex){
            ex.printStackTrace();
    }
    }
}
