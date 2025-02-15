package com.example;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class DocumentSimilarityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private Text Key = new Text();      
    private Text fileValue = new Text(); 

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        
        FileSplit file = (FileSplit) reporter.getInputSplit();
        String fileName = file.getPath().getName();
        fileValue.set(fileName); 

        
        StringTokenizer token = new StringTokenizer(value.toString());

        while (token.hasMoreTokens()) {
            
            String word = token.nextToken().replaceAll("[^a-zA-Z0-9]", "").toLowerCase();

            
            if (!word.isEmpty()) {
                Key.set(word); 
                output.collect(Key, fileValue); 
            }
        }
    }
}