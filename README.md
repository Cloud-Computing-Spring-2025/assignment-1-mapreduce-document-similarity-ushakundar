# Document Similarity using Hadoop MapReduce
## Objective 
This project implements a document similarity computation using Hadoop MapReduce. It calculates the Jaccard Similarity between pairs of text documents stored in HDFS.
## Approach and Implementation

### Mapper

The Mapper reads two input documents and tokenizes their content into words. It then constructs sets of unique words for each document and emits them as key-value pairs, where the key is the document pair identifier and the value is the set of words.

### Reducer
The Reducer receives the word sets for each document pair and computes the Jaccard Similarity = |A ∩ B| / |A ∪ B|
Where:
- `|A ∩ B|` is the number of words common to both documents
- `|A ∪ B|` is the total number of unique words in both documents
The final similarity score is written to the output file in HDFS.

## Running the Project

#### Prerequisites

Ensure you have Hadoop installed and running. The input text files should be stored in HDFS.

## Steps to Run

### 1. **Start the Hadoop Cluster**

Run the following command to start the Hadoop cluster:

```bash
docker compose up -d
```

### 2. **Build the Code**

Build the code using Maven:

```bash
mvn clean package
```

### 3. **Copy JAR to Docker Container**

Copy the JAR file to the Hadoop ResourceManager container:

```bash
docker cp target/DocumentSimilarity-0.0.1-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 5. **Move Dataset to Docker Container**

Copy the dataset to the Hadoop ResourceManager container:

```bash
docker cp input resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
### 6. **Connect to Docker Container**

Access the Hadoop ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Navigate to the Hadoop directory:

```bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 7. **Set Up HDFS**

Create a folder in HDFS for the input dataset:

```bash
hadoop fs -mkdir -p /input/dataset
```

Copy the input dataset to the HDFS folder:

```bash
hadoop fs -put ./input /input/dataset
```

### 8. **Execute the MapReduce Job**

Run your MapReduce job using the following command:

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/DocumentSimilarity-0.0.1-SNAPSHOT.jar com.example.controller.Controller /input/dataset/input.txt /output
```

### 9. **View the Output**

To view the output of your MapReduce job, use:

```bash
hadoop fs -cat /output/*
```

### 10. **Copy Output from HDFS to Local OS**

To copy the output from HDFS to your local machine:

1. Use the following command to copy from HDFS:
    ```bash
    hdfs dfs -get /output /opt/hadoop-3.2.1/share/hadoop/mapreduce/
    ```

2. use Docker to copy from the container to your local machine:
   ```bash
   exit 
   ```
    ```bash
    docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output/ shared-folder/output/
    ```

### **📥 Example Input**  

You will be given multiple text documents. Each document will contain several words. Your task is to compute the **Jaccard Similarity** between all pairs of documents based on the set of words they contain.  

#### **Example Documents**  

##### **doc1.txt**  
```
hadoop is a distributed system
```

##### **doc2.txt**  
```
hadoop is used for big data processing
```

##### **doc3.txt**  
```
big data is important for analysis
```



## Example Calculation

Consider two documents:
 
**doc1.txt words**: `{hadoop, is, a, distributed, system}`
**doc2.txt words**: `{hadoop, is, used, for, big, data, processing}`

- Common words: `{hadoop, is}`
- Total unique words: `{hadoop, is, a, distributed, system, used, for, big, data, processing}`

Jaccard Similarity calculation:
```
|A ∩ B| = 2 (common words)
|A ∪ B| = 10 (total unique words)

Jaccard Similarity = 2/10 = 0.2 or 20%
```

## Use Cases

Jaccard Similarity is commonly used in:
- Document similarity detection
- Plagiarism checking
- Recommendation systems
- Clustering algorithms


### **📤 Expected Output**  

The output should show the Jaccard Similarity between document pairs in the following format:  
```
(doc1, doc2) -> 60%  
(doc2, doc3) -> 50%  
```

---


## Challenges Faced and Solutions

### 1. Handling More than Two Input Files

Problem: The program only supports two input files at a time.

Solution: Instead of modifying the existing program, we ran the job multiple times for different pairs.

### 2. Input Path Issues

Problem: Initial attempts failed because the input files were placed inside a subdirectory (/input/dataset/input/).

Solution: Verified file locations using hadoop fs -ls /input/dataset/input and corrected the paths.

### 3. Output Directory Conflicts

Problem: Running the job multiple times caused errors due to existing output directories.

Solution: Used hadoop fs -rm -r /output_X before each run.
