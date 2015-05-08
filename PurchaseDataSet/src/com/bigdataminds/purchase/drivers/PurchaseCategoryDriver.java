package com.bigdataminds.purchase.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bigdataminds.purchase.mappers.PurchaseCategoryMapper;
import com.bigdataminds.purchase.reducers.PurchaseCategoryReducer;

public class PurchaseCategoryDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		// TODO explore JobBuilder class.
		
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
			getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Purchase Dataset category job");
		job.setJarByClass(PurchaseCategoryDriver.class);
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(PurchaseCategoryMapper.class);
		job.setReducerClass(PurchaseCategoryReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println(conf);

		boolean jobSucceeded = job.waitForCompletion(true);
		if (jobSucceeded) {
			return 0;
		} else {
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PurchaseCategoryDriver(), args);
		System.exit(exitCode);
	}

}
