package com.bigdataminds.hive.jdbc.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveConnection {

	Connection con = null;
	Statement st;
	ResultSet rs;

	public HiveConnection() {
		System.out.println("Inside constructor of HiveConnection class");
	}

	public boolean testSelect() throws SQLException {
		System.out.println("Inside testSelect() method");
		boolean isavailable = false;
		String selectQuery = new String("select count(*) from test.stocks");
		st = connection().createStatement();
		rs = st.executeQuery(selectQuery);

		while (rs.next()) {
			System.out.println("inside while loop");
			System.out.println(rs.getString(1));
			return isavailable = true;
		}
		return isavailable;

	}

	public Connection connection() {
		System.out.println("Inside connection() method");
		try {
			Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException c) {
			System.out.println("Class not found!");
		}
		try {

			con = DriverManager.getConnection("jdbc:hive://localhost:10000/test", "", "");

		} catch (SQLException s) {
			s.printStackTrace();
			System.out.println("Connection is not found!");
		}
		return con;
	}

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub

		System.out.println("Inside main method");
		HiveConnection conntest = new HiveConnection();
		boolean test = conntest.testSelect();

		System.out.println("record is there in database :" + test);

	}

}