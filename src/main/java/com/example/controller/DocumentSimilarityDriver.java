package com.example.controller;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.example.DocumentSimilarityMapper;
import com.example.DocumentSimilarityReducer;

public class DocumentSimilarityDriver {
    
    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.err.println("Please provide the correct usage:");
            System.exit(1);
        }
    
        JobConf config = new JobConf(DocumentSimilarityDriver.class);
        config.setJobName("Count");
        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(Text.class);
        config.setMapperClass(DocumentSimilarityMapper.class);
        // conf.setCombinerClass(DocumentSimilarityReducer.class);
        config.setReducerClass(DocumentSimilarityReducer.class);
        config.setInputFormat(TextInputFormat.class);
        config.setOutputFormat(TextOutputFormat.class);
    
        FileInputFormat.setInputPaths(config, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(config, new Path(args[2]));
    
        JobClient.runJob(config);
    }
    
}