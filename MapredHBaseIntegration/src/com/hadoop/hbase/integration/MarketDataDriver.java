package com.hadoop.hbase.integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class MarketDataDriver extends Configured implements Tool {
	   @Override
	    public int run(String[] args) throws Exception { 

	        Configuration conf = new Configuration();
	        Job job = new Job(conf, "AAPL Market Data Process Job");
	        job.setJarByClass(MarketDataProcess.class);
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TableOutputFormat.class);
	        
	        job.setMapperClass(MarketDataMapper.class);
	        job.setReducerClass(MarketDataReducer.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(MapWritable.class);

	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        TableMapReduceUtil.initTableReducerJob("aapl_marketdata",
	                MarketDataReducer.class, job);

	        boolean jobSucceeded = job.waitForCompletion(true);
	        if (jobSucceeded) {
	            return 0;
	        } else {
	            return -1;
	        }
	    }

	}