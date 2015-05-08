package com.bigdataminds.purchase.mappers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PurchaseCategoryMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		System.out.println("=======================================================");
		System.out.println(context.getInputSplit());
		System.out.println(context.getInputSplit().getLocations());
		System.out.println(context.getInputSplit().getLength());
		System.out.println("=======================================================");
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String purchase = value.toString();
		String[] purchaseData = purchase.split("\t");
		if (purchaseData.length == 6) {
			try {
				context.write(new Text(purchaseData[3]), new DoubleWritable(Double.valueOf(purchaseData[4])));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}			
		}
	}

}
