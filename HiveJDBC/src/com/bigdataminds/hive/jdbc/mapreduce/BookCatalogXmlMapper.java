package com.bigdataminds.hive.jdbc.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import static javax.xml.stream.XMLStreamConstants.*;

public class BookCatalogXmlMapper  extends Mapper<LongWritable, Text, Text, NullWritable>{
	
/*
    protected void setup(
            Context context)
            throws IOException, InterruptedException {
          //context.write(new Text("BookID"+" | "+"Author"+" | "+"Title"+" | "+"Genre"+" | "+"Publication_Date"+" | "+"Description"), null);
          context.write(new Text("BookID"+" | "+"Author"+" | "+"Title"+" | "+"Genre"+" | "+"Price"+" | "+"Publication_Date"+" | "+"Description"+" | "), null);
        }

        
   protected void cleanup(
            Context context)
            throws IOException, InterruptedException {
          context.write(null, null);
        }
        
 */

	public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
		String document = value.toString();
		System.out.println("'" + document + "'");
		System.out.println("===================");
		System.out.println("key is:"+key + "val is :"+value);
		try {
		XMLStreamReader reader =  XMLInputFactory.newInstance().createXMLStreamReader(new
		     ByteArrayInputStream(document.getBytes()));
		String id="";
		String author = "";
		String title = "";
		String genre = "";
		String price = "";
		String publish_date = "";
		String description = "";	
		String currentElement = "";
		
		String finalString = "";
		
		StringBuilder sb = new StringBuilder();
		
		while (reader.hasNext()) {
		int code = reader.next();
		//System.out.println("code value is:"+code);
		switch (code) {
		 case START_ELEMENT:
		   currentElement = reader.getLocalName();
		   
		   System.out.println("currentElement:"+currentElement);
		   
		   break;
		  
		 		 
		 case CHARACTERS:
			 // Need to add condition for id
		   if (currentElement.equalsIgnoreCase("id")) {
				   id += reader.getText().toString().trim();
				   System.out.println("id is:"+id);			   	   
				   
		   } 
		   else if (currentElement.equalsIgnoreCase("author")) {
			   author += reader.getText().toString().trim();
			   System.out.println("author is:"+author);			   	   
			   
		   } else if (currentElement.equalsIgnoreCase("title")) {
			   title += reader.getText().toString().trim();			   
			   System.out.println("title is:"+title);			   
			   
		   }else if (currentElement.equalsIgnoreCase("genre")) {
			   genre += reader.getText().toString().trim();			   
			   System.out.println("genre is:"+genre);			   	
			   
		   } else if (currentElement.equalsIgnoreCase("price")) {
			   price += reader.getText().toString().trim();
			   //finalString += price+"\t";
			   System.out.println("price is:"+price);
			   			  
		   } else if (currentElement.equalsIgnoreCase("publish_date")) {
			   publish_date += reader.getText().trim();
			   System.out.println("publish_date is:"+publish_date);
			   
			   
		   } else if (currentElement.equalsIgnoreCase("description")) {
			   description += reader.getText().trim();
			   //finalString += description;
			   System.out.println("description is:"+description);
			   
			   
		   }
		  
		   break;
		}
		}
		reader.close();
		//Append all data
		
		
 	//finalString = id.trim()+" | "+author.trim()+" | " + title.trim()+" | " + genre.trim()+" | " + price.trim()+" | " + publish_date.trim()+" | " + description.trim();
 	
 	finalString = id.trim()+"|"+author.trim()+"|" + title.trim()+"|" + genre.trim()+"|" + price.trim()+"|" + publish_date.trim()+"|" + description.trim();

		
		
		System.out.print("finalString:"+finalString);
		context.write(new Text(finalString.toString().trim()), null);
		} catch (Exception e) {
		System.out.println(e);
		System.err.println("Error processing '" + document + "'");
		}
}



}
