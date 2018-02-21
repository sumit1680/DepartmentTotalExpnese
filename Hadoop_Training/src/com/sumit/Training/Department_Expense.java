package com.sumit.Training;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Department_Expense {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String fields[] = line.split(",");
			
			String dept = fields[1];
			int expenses = Integer.parseInt(fields[2]);
			
			context.write(new Text(dept), new IntWritable(expenses));
		}
	}
		
	
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		
	
		public void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			
			for (IntWritable x : values) {
				
				sum+= x.get();
				
				
			}
			
			context.write(key, new IntWritable(sum));
			
			
		}
	}
		
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration ();
		Job job = Job.getInstance(conf,"Department_Expense");
		
		job.setJarByClass(Department_Expense.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
