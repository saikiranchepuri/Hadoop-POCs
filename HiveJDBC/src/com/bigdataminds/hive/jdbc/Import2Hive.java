package com.bigdataminds.hive.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.bigdataminds.hive.jdbc.connection.HiveConnection;

public class Import2Hive {

	Connection con = null;
	HiveConnection hiveCon = null;
	Statement st = null;
	ResultSet rs = null;
	static String databaseName = "test";

	public Import2Hive() {
		hiveCon = new HiveConnection();
	}

	
	public void createTable() throws SQLException {
		System.out.println("inside createTable.....");
		String selectQuery = new String("create table test.BookCatalog(bookid string,author string,title string,genre string,price string,publish_date string,description string) ROW FORMAT delimited fields terminated by '|' lines terminated by '\n' stored as textfile");
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
			while (rs.next()) {
				// System.out.println("inside while loop");
				System.out.println(rs.getString(1));

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (con != null) {
				con.close();
			}
		}

	}
	

	public void loadDataIntoTable(String path) throws SQLException {
		System.out.println("inside loadDataIntoTable.....");
		//String path="/home/hadoopuser/workspace_2/Hive_Jdbc_Project/output/bookcatalog/part-m-00000";
		
		String selectQuery = new String("LOAD DATA LOCAL INPATH \'"+path+"\' INTO Table test.BookCatalog");
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
			while (rs.next()) {
				// System.out.println("inside while loop");
				System.out.println(rs.getString(1));

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
				}
			if (con != null) {
				con.close();
			}
		}

	}
	

	public void retrieveAllDataFromTable() throws SQLException {
		System.out.println("inside retrieveAllDataFromTable.....");
				
		String selectQuery = new String("select * from test.BookCatalog");
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
			while (rs.next()) {
				// System.out.println("inside while loop");
				System.out.print(rs.getString(1)+"\t");
				System.out.print(rs.getString(2)+"\t");
				System.out.print(rs.getString(3)+"\t");
				System.out.print(rs.getString(4)+"\t");
				System.out.print(rs.getString(5)+"\t");
				System.out.print(rs.getString(6)+"\t");
				System.out.println(rs.getString(7));

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (con != null) {
				con.close();
			}
		}

	}
	

	public void retrieveSpecificDataFromTable() throws SQLException {
		System.out.println("inside retrieveSpecificDataFromTable.....");
				
		String selectQuery = new String("select * from test.BookCatalog where author='Corets, Eva'");
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
			while (rs.next()) {
				// System.out.println("inside while loop");
				System.out.print(rs.getString(1)+"\t");
				System.out.print(rs.getString(2)+"\t");
				System.out.print(rs.getString(3)+"\t");
				System.out.print(rs.getString(4)+"\t");
				System.out.print(rs.getString(5)+"\t");
				System.out.print(rs.getString(6)+"\t");
				System.out.println(rs.getString(7));

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (con != null) {
				con.close();
			}
		}

	}
	
	
	
	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub

		Import2Hive imp2hive = new Import2Hive();
		imp2hive.createTable();
		imp2hive.loadDataIntoTable(args[0]);
		imp2hive.retrieveAllDataFromTable();		
		imp2hive.retrieveSpecificDataFromTable();

	}

}
