package com.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class DocumentSimilarityReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private HashSet<String> union = new HashSet<>(); 
    private HashSet<String> intersection = new HashSet<>(); 
    private String fileName1 = "", fileName2 = "";

    private OutputCollector out;
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        this.out=output;        
        HashSet<String> document = new HashSet<>(); 

        // Collect document names for the current word
        while (values.hasNext()) {
            document.add(values.next().toString());
        }

        // Track the two document names
        if (fileName1.isEmpty() && !document.isEmpty()) {
            fileName1 = document.iterator().next();
        } 
        if (fileName2.isEmpty() && document.size() > 1) {
            for (String doc : document) {
                if (!doc.equals(fileName1)) {
                    fileName2 = doc;
                    break;
                }
            }
        }

        // Add the word to the unique words set (A ∪ B)
        union.add(key.toString());

        // If the word appears in both documents, add it to common words (A ∩ B)
        if (document.size() == 2) {
            intersection.add(key.toString());
        }
    }

    @Override
    public void close() throws IOException {
        // Compute Jaccard Similarity = |A ∩ B| / |A ∪ B|
        double Similarity = union.isEmpty() ? 0.0 : (double) intersection.size() / union.size();

        // Output document names being compared
        out.collect(new Text("Comparing Documents:"), new Text(fileName1 + " and " + fileName2));

        // Output the list of common words
        out.collect(new Text("Common Words (" + intersection.size() + "):"), new Text(String.join(", ", intersection)));

        // Output the total unique words across both documents
        out.collect(new Text("Unique Words (A ∪ B) (" + union.size() + "):"), new Text(String.join(", ", union)));

        // Output the Jaccard Similarity score
        out.collect(new Text("Jaccard Similarity:"), new Text(String.format("%.2f", Similarity)));
    }
}