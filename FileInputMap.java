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

public class FileInputMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1); //Dummy variable useless
        private Text word = new Text();
        private ArrayList<String> mylist= new ArrayList<String>();
        public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
                String line = value.toString();
                word.set(line);
                context.write(word, one);        
        }
    }