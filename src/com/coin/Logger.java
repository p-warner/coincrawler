package com.coin;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Handles all logging into the database. It's an extended database manager. 
 * 
 * Logging can be accomplished by inserting the line-
 * new Logger().log(LogType.WARNING,  super.scraperId+":getStatus("+this.url+")=Null connection.");
 * 
 * @author Phil
 *
 */
public class Logger extends DatabaseManager{

	
	/**
	 * Connects to a local MySQL
	 */
	private void connect(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			super.conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/coin", DB_USER, DB_PASSWORD);
		}catch (Exception e) {
			System.out.println("Complete failure. Can't connect to database.");
		}finally{
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
	}
	
	/**
	 * Inserts an item into the log. Follow... section.class:methodName(args)=results
	 * @param	type	the type of message
	 */
	public void log(String type, String message){
		try {
			this.connect();
			ps = super.conn.prepareStatement("INSERT INTO coin.log(type, message) VALUES (?, ?);");
			ps.setString(1, type.toString());
			ps.setString(2, message);
			ps.executeUpdate();
		}catch(SQLException e){
			System.out.println("Logger can't log. ");
			e.printStackTrace();
		}finally{
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
	}
}
