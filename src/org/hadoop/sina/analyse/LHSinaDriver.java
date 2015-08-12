package org.hadoop.sina.analyse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;

public class LHSinaDriver
{
	///////////////////////////////////////////////////////////////////////////////
	public static void main(String[] args) throws Exception 
	{   
	    if (args.length < 3) 
	    {
	        System.err.println("Usage: LHSinaDriver <in> [<in>...] <out>");
	        System.exit(2);
	    }
		
	    ///输入路径
	    Path inputPath  = new Path(args[0]);
	    
	    Path tmpPath    = new Path(args[1]);
	    ///输出路径
	    Path outputPath = new Path(args[2]);
	    
	    ///top k 数值
	    String topK     = args[3];
	    
	    boolean result = true;
	    Job counterJob = getCounterJob(inputPath, tmpPath);
	    if (counterJob != null)
	    {
	    	result = counterJob.waitForCompletion(true);
	    	
	    	if (result)
	    	{
	    		Job topKJob = getTopKJob(tmpPath, outputPath, topK);
	    		if (topKJob != null)
	    		{
	    			result = topKJob.waitForCompletion(true);
	    		}
	    	}
	    }
	    
	    System.exit(result ? 0 : 1);
	}
	
	///创建统计总数作业
	public static Job getCounterJob(Path aInputPath, Path aOutputPath)
	{
	    Configuration conf = new Configuration();
	    ///设置输出分隔符
	    conf.set("mapred.textoutputformat.separator", ";");	    
	    ///设置Topk
	    conf.set("topk", "20");
	    Job job = null;
		try 
		{
			job = Job.getInstance(conf, "LHCounterJob");
		    job.setJarByClass(LHSinaDriver.class);
		    job.setMapperClass(LHSinaCalculateMapper.class);

		    job.setCombinerClass(LHSinaCalculateReducer.class);
		    job.setReducerClass(LHSinaCalculateReducer.class);	    
		    		       
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.setInputPaths(job, aInputPath);
		    FileOutputFormat.setOutputPath(job, aOutputPath);
		}
		catch (IOException e)
		{
			return null;
		}	    
		
	    return job;
	}
	
	///创建排序TopK总数作业
	public static Job getTopKJob(Path aInputPath, Path aOutputPath, String aTopK)
	{
	    Configuration conf = new Configuration();
	    ///设置输出分隔符
	    conf.set("mapred.textoutputformat.separator", ";");
	    ///设置Topk
	    conf.set("topk", aTopK);
	    Job job = null;
		try 
		{
			job = Job.getInstance(conf, "LHTopKJob");
		    job.setJarByClass(LHSinaDriver.class);
		    job.setMapperClass(LHSinaTopKMapper.class);
		    job.setReducerClass(LHSinaTopKReducer.class);	    
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setMapOutputKeyClass(NullWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setNumReduceTasks(1);
		    FileInputFormat.setInputPaths(job, aInputPath);
		    FileOutputFormat.setOutputPath(job, aOutputPath);
		}
		catch (IOException e)
		{
			return null;
		}	    
		
	    return job;
	}
}

