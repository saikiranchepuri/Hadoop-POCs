package com.hadoop.hbase.integration;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import com.cedarsoftware.util.io.JsonWriter;

public class MarketDataReducer extends TableReducer<Text, MapWritable, ImmutableBytesWritable>{

@Override
   public void reduce(Text arg0, Iterable<MapWritable> arg1, Context context) {
   // Since the complex key made up of stock symbol and date is unique
   // one value comes for a key.
System.out.println("inside Reducer.......");
   MapWritable marketData = null;
   for (MapWritable value : arg1) {
       marketData = value;
       break;
   }

   ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(arg0.toString()));
   System.out.println("key is:"+key);
   Put putrecords = new Put(Bytes.toBytes(arg0.toString()));

   putrecords.add(Bytes.toBytes("marketdata"), Bytes.toBytes("daily"), Bytes.toBytes(JsonWriter.toJson(marketData)));
   
   try {
	   if(putrecords instanceof Put)
	   {
		   System.out.println("put is instanceof put");
	   }
	   System.out.println("key:"+key +"value is:"+putrecords);
       context.write(key, putrecords);
   } catch (IOException e) {
       // TODO Auto-generated catch block
   } catch (InterruptedException e) {
       // TODO Auto-generated catch block
   }
  }
}