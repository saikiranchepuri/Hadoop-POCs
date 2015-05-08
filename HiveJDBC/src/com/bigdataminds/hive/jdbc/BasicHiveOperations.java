
package com.bigdataminds.hive.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.bigdataminds.hive.jdbc.connection.HiveConnection;

public class BasicHiveOperations {

	Connection con = null;
	HiveConnection hiveCon = null;
	Statement st = null;
	ResultSet rs = null;
	static String databaseName = "test";

	public BasicHiveOperations() {
		hiveCon = new HiveConnection();
	}

	/*
	 * Method to show Hive databases
	 */

	public void showDatabases() throws SQLException {
		System.out.println("inside showDatabases.....");
		String selectQuery = new String("show databases");
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
			while (rs.next()) {
				// System.out.println("inside while loop");
				System.out.println(rs.getString(1));

			}
		} catch (SQLException e) {
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

	
	/*
	 * Method to show Hive Tables in a specific database
	 */

	public void showTables(String databaseName) throws SQLException {
		System.out.println("inside showTables.....and dbname is:"+databaseName);
		//String selectQuery1 = new String("use "+databaseName);
		//String selectQuery2 = new String("show tables");
		String selectQuery = new String("show tables in "+databaseName);
				
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
		
			while (rs.next()) {
					// System.out.println("inside while loop");
					System.out.println(rs.getString(1));

				}		
			
		} catch (SQLException e) {
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



	public void describeTable() throws SQLException {
		System.out.println("inside describeTable.....");
		String selectQuery = new String("show databases");
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
			while (rs.next()) {
				// System.out.println("inside while loop");
				System.out.println(rs.getString(1));

			}
		} catch (SQLException e) {
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
	

	public void createTable() throws SQLException {
		System.out.println("inside createTable.....");
		String selectQuery = new String("create table test.firsttable_23(name string,ts string,query string) ROW FORMAT delimited fields terminated by '\t' lines terminated by '\n' stored as textfile");
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
			while (rs.next()) {
				// System.out.println("inside while loop");
				System.out.println(rs.getString(1));

			}
		} catch (SQLException e) {
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
	

	public void loadDataIntoTable() throws SQLException {
		System.out.println("inside loadDataIntoTable.....");
		String path="/home/hadoopuser/Downloads/HIVE/input/excite-small.log";
		
		String selectQuery = new String("LOAD DATA LOCAL INPATH '/home/varma/workspace/HiveJDBC/input/excite-small.log' INTO Table test.firsttable_23");
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
			while (rs.next()) {
				// System.out.println("inside while loop");
				System.out.println(rs.getString(1));

			}
		} catch (SQLException e) {
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
	

	public void retrieveDataFromTable() throws SQLException {
		System.out.println("inside retrieveDataFromTable.....");
				
		String selectQuery = new String("select * from test.firsttable_23 where name='128315306CE647F6'");
		try {
			con = hiveCon.connection();
			st = con.createStatement();
			rs = st.executeQuery(selectQuery);
			while (rs.next()) {
				// System.out.println("inside while loop");
				System.out.print(rs.getString(1)+"\t");
				System.out.print(rs.getString(2)+"\t");
				System.out.println(rs.getString(3));

			}
		} catch (SQLException e) {
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

		BasicHiveOperations bopts = new BasicHiveOperations();
		bopts.showDatabases();
		bopts.showTables(databaseName);
		bopts.createTable();
		bopts.loadDataIntoTable();
		bopts.retrieveDataFromTable();

	}

}
