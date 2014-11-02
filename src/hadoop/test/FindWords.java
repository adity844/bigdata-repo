package hadoop.test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("deprecation")
public class FindWords {
	static enum CountersEnum {INPUT_WORDS};
	static Counters counters;
	static Counter counter;
	static long tot = 0;
	static int total = 0;
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);	
		
		Job job = new Job(conf);
		job.setJarByClass(FindWords.class);
		job.setMapperClass(FindWords.FindWordsMapper.class);
		job.setReducerClass(FindWords.FindWordsReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path("Input"));
		Path outputDir = new Path ("Output");
		fs.delete(outputDir,true);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.waitForCompletion(true);
//		counters = job.getCounters();
		//2nd phase
		Job job2 = new Job(conf);
		job2.setJarByClass(FindWords.class);
		//job2.setMapperClass(IdentityMapper.class);
		//job2.setMapperClass(FindWords.VowelPercentMapper.class);
		job2.setReducerClass(FindWords.VowelPercentReducer.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job2, outputDir);
		Path outputDir2 = new Path ("Output2");
		fs.delete(outputDir2,true);
		FileOutputFormat.setOutputPath(job2, outputDir2);

		job2.waitForCompletion(true);
		
	}

	public static class FindWordsMapper extends Mapper <LongWritable,Text,Text,Text>{
		
		String str, aword = new String ();
		StringTokenizer tokens ;
		Text eachVow, eachWord;
		//String words = new Str
		
		public void map (LongWritable offset,Text line,Context ctx) throws IOException, InterruptedException{
			str = line.toString();

			if (str != " "){
				tokens = new StringTokenizer(str, " ");
				
				while (tokens.hasMoreTokens()){
					aword = tokens.nextToken().toUpperCase();
					if ((aword.charAt(0) == 'A') || (aword.charAt(0) == 'E') || (aword.charAt(0) == 'I') || (aword.charAt(0) == 'O') || (aword.charAt(0) == 'U')){
						eachVow = new Text(Character.toString(aword.charAt(0)));
						//System.out.println(aword);
						ctx.write(eachVow, new Text(aword) );
								 
						ctx.getCounter(CountersEnum.INPUT_WORDS).increment(1);
						
					    tot = ctx.getCounter(CountersEnum.INPUT_WORDS).getValue();
						//counters.findCounter(groupName, counterName)
					  //   counter.increment(1);
					     System.out.println(tot);
					}
				}
			}

		}
	}
	public static class FindWordsReducer extends Reducer <Text,Text,Text,Text>{
		String eachWord, wordList = new String();
		int count=0;
		
		public void reduce(Text vowel,Iterable<Text> words,Context ctx) throws IOException, InterruptedException{
			//			eachWord = words.toString();
			wordList = " ";
			count = 0;
			
			for (Text word :words){
				count++;
				wordList += word.toString() + ", ";
				total++;
			}
		//	Counter counter = ctx.getCounter(CountersEnum.class.getName());
			
			wordList += " -- " + Integer.toString(count) + " -- " + Integer.toString(total)  ;
		
			ctx.write(vowel, new Text (wordList));
		}
	}
/*	public static class VowelPercentMapper extends Mapper <LongWritable,Text,Text,Text>{
	
	} */
	public static class VowelPercentReducer extends Reducer <LongWritable,Text,Text,FloatWritable>{
		String str, str2, str3, vowel = new String();
		float percent = 0.0f;
		int count, indx, indx1,indx2 = 0;
		
		public void reduce(LongWritable sp,Iterable<Text> words,Context ctx) throws IOException, InterruptedException{		
			for (Text word :words){
				str = word.toString();
				indx = str.indexOf(" ");
				vowel = str.substring(0, indx);
				indx1 = str.indexOf(" -- ");
				indx2 = str.lastIndexOf(" -- ");
				str2 = str.substring(indx1+4, indx2).trim();				
				System.out.println(str2);
				percent = (Integer.parseInt(str2))*100/tot;
				//indx = str.lastIndexOf(" -- ");
				//str3 = str.substring(indx+1);
				//totalCount.add(Integer.parseInt(str3));
				
			}	
			
			ctx.write(new Text (vowel+"%"), new FloatWritable(percent));

		}
	}
}
