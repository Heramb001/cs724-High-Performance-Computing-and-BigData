package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//package to implement FileSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import more packages for file system
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
//java utils
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;

public class InvertIndex extends Configured implements Tool {

public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new InvertIndex(), args);
	
	//print the output data
	Map<String, String> data = readDirectory(new Path(args[1]));
	for (Map.Entry<String, String> entry : data.entrySet()) {
		System.out.println(entry.getKey() + ", " + entry.getValue());
	}
	System.exit(res);
}

public int run(String[] args) throws Exception {
	Job job = Job.getInstance(getConf(), "invertindex");
	job.setJarByClass(this.getClass());
	// Use TextInputFormat, the default unless job.setInputFormatClass is used
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setMapperClass(Maper.class);
	job.setReducerClass(Reduce.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	return job.waitForCompletion(true) ? 0 : 1;
}

public static class Maper extends Mapper<LongWritable, Text, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private long numRecords = 0;    
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	@Override
	public void map(LongWritable offset, Text lineText, Context context)
	throws IOException, InterruptedException {
		//fetch the file name 
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		String line = lineText.toString();
		Text currentWord = new Text();
		for (String word : WORD_BOUNDARY.split(line)) {
			if (word.isEmpty()) {
				continue;
				}
			currentWord = new Text(word);
			context.write(currentWord,new Text(fileName));
		}
	}
}

public static class Reduce extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text word, Iterable<Text> fileNames, Context context)
	throws IOException, InterruptedException {
		String files = new String();
		boolean firstEntry = true;
		for (Text fileName : fileNames) {
			if (firstEntry){
				firstEntry = false;
			}else{
				files=files+",";
			}
			files=files+fileName.toString();
			//if (files.lastIndexOf(fileName.toString()) < 0) {
			//	files.append(fileName.toString());
			//}
		}
		context.write(word, new Text(files.toString()));
	}
}

	//function to read the file from the directory
	public static Map<String, String> readDirectory(Path path) throws IOException{
		Map<String, String> map = new HashMap<String, String>();
		FileSystem fs;
		fs = FileSystem.get(new Configuration());
		try {
			FileStatus[] stat = fs.listStatus(path);
			for (int i = 0; i < stat.length; i++) {
				// skip '_SUCCESS' directory
				if (stat[i].getPath().getName().startsWith("_")) {
					continue;
				}

				Map<String, String> pairs = readFile(
						stat[i].getPath(), fs);
				map.putAll(pairs);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error reading the file system!");
		}
		
		return map;
	}
	
	//function to read datafile
	public static Map<String, String> readFile(Path path, FileSystem fs) throws IOException{
		Map<String, String> map = new HashMap<String, String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		try {
			String line;
			line = br.readLine();
			while (line != null) {
				String[] tokens = line.split("\\s+");
				if (tokens.length != 2) {
					throw new IOException("Error parsing the line: expect 2 terms delimited by tab!");
				}
				String value = new String(tokens[0]);
				String file = new String(tokens[1]);
				map.put(value, file);
				line = br.readLine();
			}
		} finally {
			br.close();
		}
		return map;
	}
	
}
