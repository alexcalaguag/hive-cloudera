package com.br.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Application {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		try {
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://192.168.15.16:10000/default","cloudera", "cloudera");
			long startTime = System.nanoTime();
			ResultSet res = con.getMetaData().getTables("", "", "%", new String[] { "TABLE", "VIEW" });
			long endTime = System.nanoTime();
			System.out.println("Time Taken : "+(endTime - startTime)/1000000+"ms");
			//int cc = res.getMetaData().getColumnCount();
			//System.out.println("writing tables name");
			int cc = res.getMetaData().getColumnCount();
			while (res.next()) {
				for (int i = 1; i <= cc; i++) {
					System.out.print(res.getString(i) + ",");
				}
				System.out.println("");
			}
			

			Statement stmt = con.createStatement();

			ResultSet rs = stmt.executeQuery("Describe person");

			System.out.println("\n== Begin Query Results ======================");

			// print the results to the console
			while (rs.next()) {
				// the example query returns one String column
				System.out.println(rs.getString(1)+" - "+rs.getString(2));
			}

			System.out.println("== End Query Results =======================\n\n");

		} catch (java.sql.SQLException sqe) {
			sqe.printStackTrace(System.err);
		}

	}

}
