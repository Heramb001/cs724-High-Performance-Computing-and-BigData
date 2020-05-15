package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
// libraries used to tokenize the values
import java.util.StringTokenizer;
// libraries used for sorting the values
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
//hadoop library packages
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
//import libraries for file system
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class FrequentBigram extends Configured implements Tool {
	
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new FrequentBigram(), args);
		
		//print most frequent Bigram from the data
		Map<String, IntWritable> bigramdata = readDirectory(new Path(args[1]));
		bigramdata = sortByValue(bigramdata);
		Map.Entry<String,IntWritable> mostFrequentBigram = bigramdata.entrySet().iterator().next();
		System.out.println("\nMost frequent bigram : ");
		System.out.println(mostFrequentBigram.getKey() + "\t" + mostFrequentBigram.getValue());
		
		// exit the system
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "frequentbigram");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Maper.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Maper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text bigram = new Text();
		private long numRecords = 0;    
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		//remove all the special characters
		private static final Pattern SpecialChar = Pattern.compile("[^a-zA-Z0-9]");
		
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
		throws IOException, InterruptedException {
			String line = lineText.toString();
			StringTokenizer itr = new StringTokenizer(line);
			String prevWord = null;
			//String bigram = null;
			Text bigram = new Text();
			while(itr.hasMoreTokens()){
				String currentWord = itr.nextToken();
				//-- code for removing special chars from data( Current Word)
				Matcher match = SpecialChar.matcher(currentWord);
				while(match.find()){
					String temp = match.group();
					currentWord = currentWord.replaceAll("\\"+temp, "");
				}
				//-- check if string is empty if empty then continue
				if(currentWord.isEmpty()){
					continue;
				}//-- else attach the currentWord to previousWord to form bi gram both the words are seperated by ','
				else{
					if(prevWord != null){
						bigram = new Text(prevWord+','+currentWord);
						context.write(bigram,one);
						//bigram = prevWord+' '+currentWord;					
					}
					prevWord = currentWord;
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text bigram, Iterable<IntWritable> counts, Context context)
		throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(bigram, new IntWritable(sum));
		}
	}
	
	//function to read the file from the bigram directory
	public static Map<String, IntWritable> readDirectory(Path path) throws IOException{
		Map<String, IntWritable> map = new HashMap<String, IntWritable>();
		FileSystem fs;
		fs = FileSystem.get(new Configuration());
		try {
			FileStatus[] stat = fs.listStatus(path);
			for (int i = 0; i < stat.length; i++) {
				// skip '_SUCCESS' directory
				if (stat[i].getPath().getName().startsWith("_")) {
					continue;
				}

				Map<String, IntWritable> pairs = readFile(
						stat[i].getPath(), fs);
				map.putAll(pairs);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error reading the file system!");
		}
		
		return map;
	}
	
	//function to read bigram datafile
	public static Map<String, IntWritable> readFile(Path path, FileSystem fs) throws IOException{
		Map<String, IntWritable> map = new HashMap<String, IntWritable>();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		try {
			String line;
			line = br.readLine();
			while (line != null) {
				String[] tokens = line.split("\\s+");
				if (tokens.length != 2) {
					throw new IOException("Error parsing the line: expect 2 terms delimited by tab!");
				}
				String bigram = new String(tokens[0]);
				IntWritable count = new IntWritable(Integer.parseInt(tokens[1]));
				map.put(bigram, count);
				line = br.readLine();
			}
		} finally {
			br.close();
		}
		return map;
	}
	
	// function to sort the map by values
	public static Map<String, IntWritable> sortByValue(Map<String, IntWritable> data){
		Map<String, IntWritable> sortedMap = new LinkedHashMap<String, IntWritable>();
		data.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
		return sortedMap;	
	}		
	
	
}
