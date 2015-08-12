package org.hadoop.sina.analyse;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LHSinaCalculateReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{
		int totalCount = 0;
		
		for(IntWritable val : values)
		{
			totalCount += val.get();
		}
		
		///输出每个用户对应的微博数据
		System.out.println(key);
		System.out.println(totalCount);			
		context.write(key, new IntWritable(totalCount));			
	}
}
