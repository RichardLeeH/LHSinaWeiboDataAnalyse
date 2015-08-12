package org.hadoop.sina.analyse;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LHSinaTopKMapper extends Mapper<Object, Text, NullWritable, Text>
{
	// Stores a map of user reputation to the record
	private TreeMap<Integer, Text> repToRecordMap = null;
	private int mTopK = 10;

	@Override
	protected void cleanup(Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException 
	{
		Iterator<Integer> ir = repToRecordMap.keySet().iterator();//获取hashMap的键值，并进行遍历
		while(ir.hasNext())
		{
		    int key = (int)ir.next();
		    Text   value = repToRecordMap.get(key);
		    String txt = value.toString()+";"+key;
		    
			context.write(NullWritable.get(), new Text(txt));		    	
		}
	}
	
	@Override
	protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		mTopK = conf.getInt("topk", 20);
		repToRecordMap = new TreeMap<Integer, Text>();
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException 
	{
		// Add this record to our map with the reputation as the key
		String []tmp = value.toString().split(";");
        repToRecordMap.put(Integer.parseInt(tmp[1]), new Text(tmp[0]));
        // If we have more than ten records, remove the one with the lowest rep
        // As this tree map is sorted in descending order, the user with
        // the lowest reputation is the last key.
        if (repToRecordMap.size() > mTopK) 
        {
        	repToRecordMap.remove(repToRecordMap.firstKey());
        }
	}
}
