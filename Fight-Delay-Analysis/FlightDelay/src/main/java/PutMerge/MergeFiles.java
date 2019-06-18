package PutMerge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MergeFiles {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


//Hadoop File System instance
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        //local File System instance
        FileSystem local = FileSystem.getLocal(conf);

        //Path to the directory in local file System
        Path inputDir = new Path(args[0]);
        Path hdfsFile = new Path(args[1]);

        try {
            //List of Files Names from FileStatus
            FileStatus[] inputFiles = local.listStatus(inputDir);
            //Writing to hdfs using OutputStream
            FSDataOutputStream out = hdfs.create(hdfsFile);

            //Reading the input files using FSDataInputStream
            for (int i = 0; i < inputFiles.length; i++) {
                FSDataInputStream in = local.open(inputFiles[i].getPath());
               // BufferedReader br = new BufferedReader(new InputStreamReader(in, UTF8));
                //int lineCount = 0;
                byte buffer[] = new byte[256];
                int bytesRead = 0;

                while ((bytesRead = in.read(buffer)) > 0 ) {

                    out.write(buffer, 0, bytesRead);
                }
                in.close();
            }
            out.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
