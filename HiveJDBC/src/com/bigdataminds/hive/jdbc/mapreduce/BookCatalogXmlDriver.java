package com.bigdataminds.hive.jdbc.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class BookCatalogXmlDriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

	    Configuration conf = new Configuration();
	    conf.set("key.value.separator.in.input.line", " ");
	    conf.set("xmlinput.start", "<book>");
	    conf.set("xmlinput.end", "</book>");

	    //conf.set("mapred.textoutputformat.separator", "|");
	    
	    Job job = new Job(conf);
	    job.setJobName("Book Catalog Xml Job...");
	    job.setJarByClass(BookCatalogXmlDriver.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    job.setMapperClass(BookCatalogXmlMapper.class);
	    
	    job.setInputFormatClass(XmlInputFormat.class);
	    
	    job.setNumReduceTasks(0);
	    job.setOutputFormatClass(TextOutputFormat.class);
	       

	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    Path outPath = new Path(args[1]);
	    FileOutputFormat.setOutputPath(job, outPath);
	    outPath.getFileSystem(conf).delete(outPath, true);

	    job.waitForCompletion(true);
	}

}
