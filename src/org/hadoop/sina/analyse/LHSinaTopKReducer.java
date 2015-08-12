package org.hadoop.sina.analyse;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LHSinaTopKReducer extends Reducer<NullWritable, Text, Text, IntWritable>
{
	/// Stores a map of user reputation to the record
	// Overloads the comparator to order the reputations in descending order 
	private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();
	private int mTopK = 10;

	@Override
	protected void setup(Reducer<NullWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		mTopK = conf.getInt("topk", 20);
	}

	@Override
	protected void cleanup(Reducer<NullWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{
		Iterator<Integer> ir = repToRecordMap.keySet().iterator();//获取hashMap的键值，并进行遍历
		while(ir.hasNext())
		{
		    int count = (int)ir.next();
		    String   value = repToRecordMap.get(count);
		    context.write(new Text(value), new IntWritable(count));
		}
	}

	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,
			Reducer<NullWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{
        for (Text value : values) 
        {
        	String []tmp = value.toString().split(";");
        	System.out.println(repToRecordMap);        	
        	System.out.println("用户名："+key.toString());
            repToRecordMap.put(Integer.parseInt(tmp[1]), tmp[0]);
            if (repToRecordMap.size() > mTopK) 
            {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }
	}
}
