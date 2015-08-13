package org.hadoop.sina.analyse;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LHSinaCalculateMapper extends Mapper<Object, Text, Text, IntWritable>
{
	private LHSinaUserModel mModel = new LHSinaUserModel();
	
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{
		String line = value.toString();
		
		if (mModel.parser(line))
		{
		    context.write(new Text(mModel.getmUserID()), new IntWritable(mModel.getSendNum()));            							
		}
	}
}

