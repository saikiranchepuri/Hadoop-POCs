package com.hadoop.hbase.integration;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MarketDataMapper extends 
    Mapper <LongWritable, Text, Text, MapWritable>{

	public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

		System.out.println("inside map......");
	final String APPL_STOCK_SYMBOL = "AAPL";

    final Text STOCK_SYMBOL = new Text("stockSymbol");
    final Text DATE = new Text("date");
    final Text STOCK_PRICE_OPEN = new Text("stockPriceOpen");
    final Text STOCK_PRICE_HIGH = new Text("stockPriceHigh");
    final Text STOCK_PRICE_LOW = new Text("stockPriceLow");
    final Text STOCK_PRICE_CLOSE = new Text("stockPriceClose");
    final Text STOCK_VOLUME = new Text("stockVolume");
    final Text STOCK_PRICE_ADJ_CLOSE = new Text("stockPriceAdjClose");

    String strLine = "";

    strLine = value.toString();
    String[] data_values = strLine.split(",");
    MapWritable marketData = new MapWritable();
    marketData.put(STOCK_SYMBOL, new Text(APPL_STOCK_SYMBOL));
    marketData.put(DATE, new Text(data_values[0]));
    marketData.put(STOCK_PRICE_OPEN, new Text(data_values[1]));
    marketData.put(STOCK_PRICE_HIGH, new Text(data_values[2]));
    marketData.put(STOCK_PRICE_LOW, new Text(data_values[3]));
    marketData.put(STOCK_PRICE_CLOSE, new Text(data_values[4]));
    marketData.put(STOCK_VOLUME, new Text(data_values[5]));
	marketData.put(STOCK_PRICE_ADJ_CLOSE, new Text(data_values[6]));
	System.out.println("key is:"+String.format("%s-%s", APPL_STOCK_SYMBOL, data_values[0]));
	
	context.write(new Text(String.format("%s-%s", APPL_STOCK_SYMBOL, data_values[0])), marketData);	    
  }
}