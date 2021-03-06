import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.lang.StringBuffer;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexBigrams {

  public static class InvertedIndexBigramsMapper extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text docID = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String input = value.toString();
  	  String[] splitinput = input.split("\\t",2);
  	  docID.set(splitinput[0]);
  	  String docContent = splitinput[1].replaceAll("[^a-zA-Z ]", " ");
	  docContent = docContent.toLowerCase();
      StringTokenizer tokenizer = new StringTokenizer(docContent);
	  
	  StringBuilder sb;
	  
	  List<String> getTokens = new ArrayList<>();
	  
      while (tokenizer.hasMoreTokens()) {
        getTokens.add((tokenizer.nextToken()).toString());
      }
	  
	  int totalTokens = getTokens.size();
	  
	  for(int i = 0; i < totalTokens - 1; i++){
		  
		  sb = new StringBuilder();
		  
		  sb.append(getTokens.get(i).toString());
		  sb.append(" ");
		  sb.append(getTokens.get(i+1).toString());
		  
		  word.set(sb.toString());
		  context.write(word, docID);
		  
	  }
	  
	  
    }
  }

  public static class InvertedIndexBigramsReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      Map<String, Integer> hmap = new HashMap<>();

      for (Text value : values) {
        String docID = value.toString();
		
		hmap.put(docID, hmap.getOrDefault(docID, 0) + 1);
      }

      StringBuffer docValue = new StringBuffer("");
      for(Map.Entry<String,Integer> entry : hmap.entrySet()) {
        docValue.append(entry.getKey() + ":" + entry.getValue() + "\t");
      }
      context.write(key, new Text(docValue.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    
	Job job = new Job();
    job.setJarByClass(InvertedIndexBigrams.class);
	job.setJobName("Inverted Index Bigrams");
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(InvertedIndexBigramsMapper.class);
    job.setReducerClass(InvertedIndexBigramsReducer.class);
    
	
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.waitForCompletion(true);
  }
}