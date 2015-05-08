package com.hadoop.hbase.integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MarketDataProcess {
    public static void main(String[] args) throws Exception {
        System.out.println("MarketDataProcess invoked");
        
        String TABLE_NAME="aapl_marketdata";
        String[] TABLE_COL_FAMILY={"marketdata"};
        // Create Table
        HBaseUtility.creatTable(TABLE_NAME, TABLE_COL_FAMILY);
        
        
    	int m_rc = 0;
        m_rc = ToolRunner.run(new Configuration(), new MarketDataDriver(), args);
        System.exit(m_rc);
        
        
    }
}