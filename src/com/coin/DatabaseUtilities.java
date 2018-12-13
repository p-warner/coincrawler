package com.coin;	

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Private class that handles various database tasks.
 * 
 * @author Phil
 *
 */
public class DatabaseUtilities{
	/**
	 * Quietly closes the connection to the database.
	 * 
	 * @param conn the connection 
	 */
	public static void close(Connection conn){
		if (conn != null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Quietly closes a PreparedStatement object.
	 * 
	 * @param ps a PreparedStatement
	 */
	public static void close(PreparedStatement ps){
		if (ps != null){
			try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Quietly closes a ResultSet object.
	 * 
	 * @param rs ResultSet
	 */
	public static void close(ResultSet rs){
		if (rs != null){
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
}
