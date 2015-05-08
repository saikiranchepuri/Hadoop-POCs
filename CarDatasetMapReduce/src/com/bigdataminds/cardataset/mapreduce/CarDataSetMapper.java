package com.bigdataminds.cardataset.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CarDataSetMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	private Text temp;
	private final static LongWritable one = new LongWritable(1);
	
	private static final int keyColumnIndex = 6;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		temp = new Text();
	}



	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(",");
		if (split != null && split.length == 7) {
			temp.set(split[keyColumnIndex]);
			context.write(temp, one);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		temp = null;
	}

}
