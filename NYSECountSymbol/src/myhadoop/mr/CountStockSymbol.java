package myhadoop.mr;


import java.io.IOException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class CountStockSymbol  {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		final Logger logger = Logger.getLogger(CountStockSymbol.class);
		Job jobj = new Job(conf);
	
		jobj.setJarByClass(myhadoop.mr.CountStockSymbol.class);
		jobj.setJobName("findsymbolcount");
		jobj.setMapperClass(myhadoop.mr.CountStockSymbol.CountStockSymbolMap.class);
		jobj.setReducerClass(myhadoop.mr.CountStockSymbol.CountStockSymbolReduce.class);	
		jobj.setCombinerClass(myhadoop.mr.CountStockSymbol.CountStockSymbolReduce.class); 
		
		FileInputFormat.setInputPaths(jobj, new Path("Input"));
		FileOutputFormat.setOutputPath(jobj, new Path("Output"));
		
		jobj.setOutputKeyClass(Text.class);
		jobj.setOutputValueClass(IntWritable.class);
		
		logger.info("Start");
		jobj.waitForCompletion(true);
		logger.info("Stop");
	}
	
	public static class CountStockSymbolMap extends Mapper<LongWritable, Text, Text, IntWritable>{		
		private IntWritable One = new IntWritable(1);
		private Text txtStckSymb = new Text();
		public void map(LongWritable recNumb, Text recLine, Context ctx) throws IOException, InterruptedException{
						
			String strRec = recLine.toString();
			int firstTabpos = strRec.indexOf("\t");
			String strStckEx = strRec.substring(0,firstTabpos);
			
			if (strStckEx.equals("NYSE")){
				int symbTabpos = strRec.indexOf("\t", firstTabpos+1);
				String strStckSymb = strRec.substring(firstTabpos+1,symbTabpos);
				txtStckSymb.set(strStckSymb);
				ctx.write(txtStckSymb, One);
			}	
			
		}
	
	}
	
	public static class CountStockSymbolReduce extends Reducer <Text,IntWritable,Text,IntWritable>{
		
		public void reduce (Text stkSymb,Iterable <IntWritable> counts,Context ctx) throws IOException, InterruptedException{
			int sum =0;
			
			for (IntWritable cnt : counts){
				sum += cnt.get();
			}
			ctx.write(stkSymb, new IntWritable(sum));
		}
	}

}
