import java.io.*;  
import java.io.IOException;
import java.util.*;
import java.util.Arrays;
import java.io.File;
import java.net.*;  
import java.io.FileNotFoundException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class testFileInput{
    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "testFileInput");
        job.setJar("testFileInput.jar");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(FileInputMap.class);
        job.setReducerClass(FileInputReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}