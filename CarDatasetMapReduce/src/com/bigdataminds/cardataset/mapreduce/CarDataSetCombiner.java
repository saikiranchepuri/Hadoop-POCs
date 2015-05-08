package com.bigdataminds.cardataset.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CarDataSetCombiner extends
		Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> value,
			Context context) throws IOException, InterruptedException {
		long sum = 0;
		Iterator<LongWritable> iterator = value.iterator();
		if (iterator != null) {
			while (iterator.hasNext()) {
				sum += iterator.next().get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
}
