/**
 * 
 * Coin's datbase manager class. Handles all CRUD items related to the database. (Exception is the logger)
 * MySQL is used as the database which must be running on localhost:3306. 
 * Also required is the MySQL jar (https://www.javatpoint.com/src/jdbc/mysql-connector.jar)
 * 
 * pwarner@pct.edu
 * 8/20/2017
 * */
package com.coin;	

import java.sql.*;
import java.util.ArrayList;

public class DatabaseManager {
	protected Connection conn;
	protected PreparedStatement ps;
	protected ResultSet rs;
	
	protected final String DB_USER = "coincrawler";
	protected final String DB_PASSWORD = "=S^2UV@Du^SErTZu#*y_JVZ";
	
	
	/**
	 * Override the default constructor.
	 */
	DatabaseManager(){
		//Don't connect now. connect when needed and close it immediately.
		//this.connect();
	}
	
	/**
	 * Closes all the connections to a database when the object is collected.
	 */
	protected void finalize(){
		DatabaseUtilities.close(conn);
		DatabaseUtilities.close(ps);
		DatabaseUtilities.close(rs);
	}
	
	/**
	 * Connects to a local MySQL service in Windows.
	 */
	private void connect(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/coin", DB_USER, DB_PASSWORD);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:connect()= Can't connect.");
		}
	}
	
	/**
	 * Retrieves list of words from the KEYS table.
	 * 
	 * @return	keywords	ArrayList of keywords contained in KEYS.
	 */
	public String[][] getKeywords(){
		String[][] keywords = new String[this.getKeywordsTotal()][2];
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT word, weight FROM coin.keys;");
			rs = ps.executeQuery();
			int i = 0;
			while(rs.next()){
				keywords[i][0] = rs.getString(1);
				keywords[i][1] = rs.getString(2);
				i++;
			}
		} catch (SQLException e) {
			System.out.println("DatabaseManager:getKeywords()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return keywords;
	}
	
	
	/**
	 * Returns the number of rows in the KEYS table
	 * 
	 * @return	count	number of rows
	 */
	public int getKeywordsTotal() {
		int count = 0;
		try {
			connect();
			ps = conn.prepareStatement("SELECT COUNT(*) FROM coin.keys;");
			rs = ps.executeQuery();
			while(rs.next()){
				count = rs.getInt(1);
			}
		} catch (SQLException e) {
			System.out.println("DatabaseManager:getKeywords()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return count;
	}
	
	/**
	 * Gets 500 latest data items from the PROCESS table.
	 * 
	 * @return	list	An list of all data in string format.
	 */
	public ArrayList<String> getProcessAggregate(int n){
		ArrayList<String> list = new ArrayList<String>();
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT title FROM coin.processed ORDER BY created_at DESC LIMIT ?;");
			ps.setInt(1, n);
			rs = ps.executeQuery();
			
			while(rs.next()){
				list.add(rs.getString(1));
			}
			
		} catch (SQLException e) {
			System.out.println("DatabaseManager:getProcessAggregate()="+e.getMessage());
			//new Logger().log(LogType.WARNING,  "getProcessAggregate()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return list;
	}
	
	/**
	 * Gets 500 latest data items from the PROCESS table.
	 * 
	 * @return	list	An list of all data in string format.
	 */
	public ArrayList<String> getProcessAggregate(){
		ArrayList<String> list = new ArrayList<String>();
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT data FROM coin.processed ORDER BY created_at DESC LIMIT 500;");
			rs = ps.executeQuery();
			
			while(rs.next()){
				list.add(rs.getString(1));
			}
			
		} catch (SQLException e) {
			System.out.println("DatabaseManager:getProcessAggregate()="+e.getMessage());
			//new Logger().log(LogType.WARNING,  "getProcessAggregate()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return list;
	}
	
	/**
	 * Returns an array list of all processed items data on a day relative to today. 0 is today, 1 is yesterday, etc...
	 * 
	 * @return	list	An list of all data in string format.
	 */
	public ArrayList<String> getDaysAgoProcessAggregate(int daysAgo){
		ArrayList<String> list = new ArrayList<String>();
		
		try {
			connect();
			if(daysAgo == 0){
				ps = conn.prepareStatement("SELECT data FROM coin.process WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 0 DAY);");
			}else{
				ps = conn.prepareStatement("SELECT data FROM coin.process WHERE created_at <= DATE_SUB(CURDATE(), INTERVAL ? DAY) AND created_at >= DATE_SUB(CURDATE(), INTERVAL ? DAY) ORDER BY created_at DESC;");
				ps.setInt(1, daysAgo);
				ps.setInt(2, ++daysAgo);
			}
			rs = ps.executeQuery();
			
			while(rs.next()){
				list.add(rs.getString(1));
			}
			
		} catch (SQLException e) {
			System.out.println("DatabaseManager:getDaysAgoProcessAggregate()="+e.getMessage());
			//new Logger().log(LogType.WARNING,  "getProcessAggregate()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return list;
	}
	
	/**
	 * Gets training data items from the TRAINING table that have a sentiment.
	 * 
	 * @return	list	An list of all data in string format.
	 */
	public String[][] getSentimentTrainingItems(){
		String[][] items = new String[this.getSentimentTrainableItemsTotal()][2];
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT title, sentiment FROM coin.training WHERE sentiment LIKE 'pos' OR sentiment LIKE 'neg' OR sentiment LIKE 'N';");
			rs = ps.executeQuery();
			int count = 0;
			while(rs.next()){
				items[count][0] = rs.getString(1);
				items[count][1] = rs.getString(2);
				count++;
			}
			
		} catch (SQLException e) {
			System.out.println("DatabaseManager:getSentimentTrainingItems()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return items;
	}
	
	/**
	 * Gets *all* training data items from the TRAINING table.
	 * 
	 * @return	list	An list of all data in string format.
	 */
	public String[][] getAllSentimentTrainingItemsForCounting(){
		String[][] items = new String[this.getAllSentimentTrainableItemsTotal()][6];
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT * FROM coin.training;");
			rs = ps.executeQuery();
			int count = 0;
			while(rs.next()){
				items[count][0] = rs.getString(1);
				items[count][1] = rs.getString(2);
				items[count][2] = rs.getString(3);
				items[count][3] = rs.getString(4);
				items[count][4] = rs.getString(5);
				items[count][5] = rs.getString(6);
				count++;
			}
			
		} catch (SQLException e) {
			System.out.println("DatabaseManager:getAllSentimentTrainingItems()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return items;
	}
	
	public int insertIntoPrediction(String prediction, String data){
		int nrs = 0;
		try {
			connect();
			ps = conn.prepareStatement("INSERT LOW_PRIORITY INTO coin.prediction(prediction, data) VALUES (?, ?);");
			ps.setString(1, prediction);
			ps.setString(2, data);
			nrs = ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:insertIntoPrediction()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return nrs;
	}
	
	/**
	 * Gets single oldest data items from the PROCESS table and the id of the record.
	 * 
	 * 
	 * @return	item	array with first index is string, second index is id in PROCESS table
	 */
	public int getProcessTotal(){
		int n = 0;
		try {
			connect();
			ps = conn.prepareStatement("SELECT COUNT(id) FROM coin.process WHERE processed = 0;");
			rs = ps.executeQuery();
			
			if(!rs.wasNull()){
				rs.next();
				n = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:getProcessSingleItem()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return n;
	}
	
	/**
	 * Gets single oldest data items from the PROCESS table and the id of the record.
	 * 
	 * 
	 * @return	item	array with first index is string, second index is id in PROCESS table
	 */
	public String[] getProcessSingleItem(){
		String[] item = new String[2];
		try {
			connect();
			ps = conn.prepareStatement("SELECT id,data FROM coin.process WHERE processed = 0 ORDER BY created_at ASC LIMIT 1;");
			rs = ps.executeQuery();
			
			if(!rs.wasNull()){
				rs.next();
				item[0] = rs.getString(2);
				item[1] = Integer.toString(rs.getInt(1));

				ps = conn.prepareStatement("UPDATE coin.process SET processed = 1 WHERE id = ?;");
				ps.setInt(1, Integer.parseInt(item[1]));
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:getProcessSingleItem()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return item;
	}
	
	
	/**
	 * Returns the totals of each category in the PROCESSED table in the past 2 days.
	 * 
	 * @return	totals	2d array with each row as a category
	 */
	public String[][] getPredictionTotals(){
		String [][] totals = new String[3][2];
		try {
			connect();
			ps = conn.prepareStatement("SELECT polarity,COUNT(polarity) FROM coin.processed AS post, coin.process AS pre WHERE pre.id = post.process_fk AND pre.created_at >= DATE_SUB(CURDATE(), INTERVAL 2 DAY) GROUP BY polarity ORDER BY polarity;");
			rs = ps.executeQuery();
			for(int i = 0; i < 3; i++){
				if(rs.next()){
					totals[i][0] = rs.getString(1);
					totals[i][1] = Integer.toString(rs.getInt(2));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:getPredictionTotals()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return totals;
	}
	/**
	 * Gets ALL unprocessed records.
	 * 
	 * 
	 * @return	item	2d-array with first index is ID, second is data in PROCESS table
	 */
	public String[][] getAllUnprocessed(){
		String[][] item = new String[getProcessTotal()][3];
		int count = 0;
		try {
			connect();
			ps = conn.prepareStatement("SELECT id,data,url FROM coin.process WHERE processed = 0 ORDER BY created_at;");
			rs = ps.executeQuery();
			
			while(rs.next()){
				item[count][0] = Integer.toString(rs.getInt(1));
				item[count][1] = rs.getString(2);
				item[count][2] = rs.getString(3);
				
				count++;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:getAllUnprocessed()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return item;
	}
	
	
	/**
	 * Sets a PROCESS record to processed = 1.
	 * 
	 * 
	 * @return	item	
	 */
	public boolean setProcessed(int id){
		try {
			connect();
			ps = conn.prepareStatement("UPDATE coin.process SET processed = 1 WHERE id = ?;");
			ps.setInt(1, id);
			ps.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:setProcessed()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return true;
	}
	
	/**
	 * Set the POLARITY of an item in PROCESSED.
	 * @param id		id of item from PROCESS
	 * @param polarity	the polarity of the item (pos, neg, N)
	 * @param url 
	 * @return			number of rows insterted
	 */
	public int setSentimentForId(int id, String polarity, String title, String url){
		int nrs = 0;
		try {
			connect();
			ps = conn.prepareStatement("INSERT LOW_PRIORITY INTO coin.processed(process_fk, polarity, title, url) VALUES (?, ?, ?, ?);");
			ps.setInt(1, id);
			ps.setString(2, polarity);
			ps.setString(3, title);
			ps.setString(4, url);
			nrs = ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:getProcessSingleItem()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return nrs;
	}
	
	/**
	 * Set the POLARITY of an item in PROCESSED.
	 * @param id		id of item from PROCESS
	 * @param polarity	the polarity of the item (pos, neg, N)
	 * @return			number of rows insterted
	 */
	public int updateTrainingItemPolarity(int id, String polarity){
		int nrs = 0;
		try {
			connect();
			ps = conn.prepareStatement("UPDATE coin.training SET sentiment = ? WHERE id = ?");
			ps.setString(1, polarity);
			ps.setInt(2, id);
			nrs = ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:updateTrainingItemPolarity()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return nrs;
	}
	
	/**
	 * Inserts data into the PROCESS table for the Analyzer to look at.
	 * 
	 */
	public void insertIntoProcess(String url, String data, String type){
		try {
			connect();
			ps = conn.prepareStatement("INSERT LOW_PRIORITY INTO coin.process(url, data, type) VALUES (?, ?, ?);");
			ps.setString(1, url);
			ps.setString(2, data);
			ps.setString(3, type);
			ps.executeUpdate();
		} catch (SQLException e) {
			System.out.println("DatabaseManager:insertIntoProcess(). Failed inserting into PROCESS table.");
			e.printStackTrace();
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
	}
	
	/**
	 * Inserts terms into PROCESSED_TRENDING and timestamps.
	 * 
	 * @return	list	An list of all data in string format.
	 */
	public int insertIntoProcessTrending(String terms){
	
		int nrs = 0;
		try {
			connect();
			ps = conn.prepareStatement("INSERT LOW_PRIORITY INTO coin.processed_trending(terms) VALUES (?);");
			ps.setString(1, terms);
			nrs = ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:insertIntoPrediction()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return nrs;
	}

	/**
	 * Places an item in the QUEUE_WEB table.
	 * 
	 * @param	ref	where the link was found
	 * @param	url	the url to scrape
	 * @return	result	number of rows inserted. 1 or 0. 0 is fail.
	 */
	public int insertIntoWebQueue(String ref, String url){
		int rowsInserted;
		try {
			connect();
			ps = conn.prepareStatement("INSERT LOW_PRIORITY INTO coin.queue_web(referrer, URL, visited) VALUES (?, ?, ?);");
			ps.setString(1, ref);
			ps.setString(2, url);
			ps.setInt(3, 0);
			rowsInserted = ps.executeUpdate();
		} catch (SQLException e) {
			System.out.println("insertIntoWebQueue(). Failed inserting into QUEUE_WEB table.");
			e.printStackTrace();
			rowsInserted = 0;
			//new Logger().log(LogType.WARNING,  "DatabaseManager:insertIntoQueue()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return rowsInserted;
	}
	
	/**
	 * Places a sample item link into the QUEUE_TRAINING table.
	 * 
	 * @param ref	the feed the link came from
	 * @param url	the link
	 * @param cat	the category of the sample
	 * @return
	 */
	public int insertIntoTrainingQueue(String ref, String url, String cat) {
		int rowsInserted;
		try {
			connect();
			ps = conn.prepareStatement("INSERT LOW_PRIORITY INTO coin.queue_training(referrer, URL, category) VALUES (?, ?, ?);");
			ps.setString(1, ref);
			ps.setString(2, url);
			ps.setString(3, cat);
			rowsInserted = ps.executeUpdate();
		} catch (SQLException e) {
			System.out.println("insertIntoTrainingQueue(). Failed inserting into QUEUE_TRAINING table. "+e.getMessage());
			rowsInserted = 0;
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return rowsInserted;
	}
	
	/**
	 * Places an item in the Queue for the Scraper.
	 * 
	 * @param	url	the of the feed
	 * @return	result	number of rows inserted
	 */
	public int insertIntoFeedPool(String url){
		int rowsInserted;
		try {
			connect();
			ps = conn.prepareStatement("INSERT LOW_PRIORITY INTO coin.pool_rss(feed) VALUES (?);");
			ps.setString(1, url);
			rowsInserted = ps.executeUpdate();
		} catch (SQLException e) {
			rowsInserted = 0;
			//new Logger().log(LogType.WARNING,  "DatabaseManager:insertIntoQueue()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return rowsInserted;
	}
	
	/**
	 * Places an item in the TRAINING table for generating sample data for training.
	 * Data going in must be well-formed sample data for doccat or sentiment analysis.
	 * 
	 * @param	title	title of item
	 * @param	sentiment	sentiment value, can be null if unknown
	 * @return	result	number of rows inserted
	 */
	public int insertIntoTraining(String title, String sentiment){
		int rowsInserted;
		try {
			connect();
			ps = conn.prepareStatement("INSERT LOW_PRIORITY INTO coin.training(title, sentiment) VALUES (?, ?);");
			ps.setString(1, title);
			ps.setString(2, sentiment);
			rowsInserted = ps.executeUpdate();
		} catch (SQLException e) {
			rowsInserted = 0;
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return rowsInserted;
	}
	
	/**
	 * Determines if the url is already in the pool.
	 * 
	 * @param	url	the url to scrape
	 * @return	result	true if its in
	 */
	public boolean inRssPool(String url){
		boolean result = false;
		try {
			connect();
			ps = conn.prepareStatement("SELECT feed FROM coin.pool_rss WHERE feed LIKE ? LIMIT 1");
			ps.setString(1, url);
			rs = ps.executeQuery();
			if(rs.next()){
				result = true;
				System.out.println("DatebaseManager:inRssPool(). There's a dup in pool_rss.");
				//new Logger().log(LogType.INFO,  "DatabaseManager:inWebQueue(). Looking up dups in queue. Possible huge query slowdown.");
			}
		} catch (SQLException e) {
			result = false;
			//new Logger().log(LogType.WARNING,  "DatabaseManager:insertIntoQueue()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return result;
	}
		
	/**
	 * Determines if the url is already in the queue.
	 * 
	 * @param	url	the url to scrape
	 * @return	result	number of rows inserted
	 */
	public boolean inWebQueue(String url){
		boolean result = false;
		try {
			connect();
			ps = conn.prepareStatement("SELECT id, url FROM coin.queue_web WHERE url LIKE ? LIMIT 1");
			ps.setString(1, url);
			rs = ps.executeQuery();
			if(rs.next()){
				result = true;
				System.out.println("DatebaseManager:inWebQueue(). There's a dup in queue_web.");
			}
		} catch (SQLException e) {
			result = false;
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return result;
	}
	
	/**
	 * Checks to see if a URL is already in QUEUE_TRAINABLE 
	 * 
	 * @param url	the url to check
	 * @return	result	false if not in, true if in
	 */
	public boolean inTrainingQueue(String url){
		boolean result = false;
		try {
			connect();
			ps = conn.prepareStatement("SELECT id, url FROM coin.queue_training WHERE url LIKE ? LIMIT 1");
			ps.setString(1, url);
			rs = ps.executeQuery();
			if(rs.next()){
				result = true;
				//System.out.println("DatabaseManager:insertIntoTrainingQueue(). There is a dup in QUEUE_TRAINING table.");
			}
		} catch (SQLException e) {
			System.out.println("DatabaseManager:insertIntoTrainingQueue(). Error checking for dup in QUEUE_TRAINING table. "+e.getMessage());
			result = false;
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		return result;
	}
	
	/**
	 * Get URL from queue. Will return first oldest uncrawled URL as string, or null string if failed.
	 * Will also set row processed value to 1 in the database.
	 * 
	 * @return	url	URL from queue in String format
	 */
	public String getFromWebQueue(){
		String url;
		int id;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT * FROM coin.queue_web WHERE visited = 0 ORDER BY created_at ASC LIMIT 1;");
			rs = ps.executeQuery();
			if(!rs.wasNull()){
				rs.next();//put cursor at row 1
				url = rs.getString(3);
				id = rs.getInt(1);
				
				ps = conn.prepareStatement("UPDATE coin.queue_web SET visited = 1 WHERE id = ?");
				ps.setInt(1, id);
				ps.executeUpdate();
			}else{
				url = null;
			}
		} catch (SQLException e) {
			url = null;
			//new Logger().log(LogType.WARNING,  "DatabaseManager:getFromQueue()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return url;
	}
	
	/**
	 * Get a single item from the QUEUE_TRAINING table.
	 * 
	 * @param cat	crypto or rando
	 * @return	url	the url.
	 */
	public String getFromTrainingQueue(String cat){
		String url;
		int id;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT * FROM coin.queue_training WHERE category = ? ORDER BY last_write ASC LIMIT 1;");
			ps.setString(1, cat);
			rs = ps.executeQuery();
			if(!rs.wasNull()){
				rs.next();//put cursor at row 1
				url = rs.getString(3);
				id = rs.getInt(1);
				
				ps = conn.prepareStatement("UPDATE coin.queue_training SET last_write = now() WHERE id = ?");
				ps.setInt(1, id);
				ps.executeUpdate();
			}else{
				url = null;
			}
		} catch (SQLException e) {
			url = null;
			System.out.println("DatabaseManagergetFromTrainingQueue(). Error getting single item from QUEUE_TRAINING table. "+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return url;
	}
	
	/**
	 * Get a trainable feed URL from the database.
	 * 
	 * @return	url	URL from feed pool in String format
	 */
	public String getTrainableFromFeedPool(){
		String url = null;
		int id;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT * FROM coin.pool_rss WHERE trainable = 1 ORDER BY last_train ASC LIMIT 1;");
			rs = ps.executeQuery();
			while (rs.next()){
				id = rs.getInt(1);
				url = rs.getString(2);
				//update 
				ps = conn.prepareStatement("UPDATE coin.pool_rss SET last_train = now() WHERE id = ?");
				ps.setInt(1, id);
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			//new Logger().log(LogType.ERROR,  "getUrlFromWebPool()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return url;
	}
	
	/**
	 * Get a feed URL from the database.
	 * 
	 * @return	url	URL from feed pool in String format
	 */
	public String getFromFeedPool(){
		String url = null;
		int id;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT * FROM coin.pool_rss ORDER BY last_crawl ASC LIMIT 1;");
			rs = ps.executeQuery();
			while (rs.next()){
				id = rs.getInt(1);
				url = rs.getString(2);
				//update 
				ps = conn.prepareStatement("UPDATE coin.pool_rss SET last_crawl = now() WHERE id = ?");
				ps.setInt(1, id);
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			//new Logger().log(LogType.ERROR,  "getUrlFromWebPool()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return url;
	}
	
	/**
	 * Get the total number of items in QUEUE_TRAINING
	 * 
	 * @return	n	Total number of RSS feeds
	 */
	public int getTrainableItemsTotal(){
		int n = 0;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT COUNT(*) FROM coin.queue_training;");
			rs = ps.executeQuery();
			while (rs.next()){
				n = rs.getInt(1);
			}
		} catch (SQLException e) {
			//new Logger().log(LogType.ERROR,  "getUrlFromWebPool()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return n;
	}
	
	/**
	 * Get the total number of items in TRAINING
	 * 
	 * @return	n	Total number of RSS feeds
	 */
	public int getSentimentTrainableItemsTotal(){
		int n = 0;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT COUNT(*) FROM coin.training WHERE sentiment LIKE 'pos' OR sentiment LIKE 'neg' OR sentiment LIKE 'N';");
			rs = ps.executeQuery();
			while (rs.next()){
				n = rs.getInt(1);
			}
		} catch (SQLException e) {
			//new Logger().log(LogType.ERROR,  "getUrlFromWebPool()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return n;
	}
	
	/**
	 * Get the total number of items in TRAINING
	 * 
	 * @return	n	Total number of RSS feeds
	 */
	public int getAllSentimentTrainableItemsTotal(){
		int n = 0;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT COUNT(*) FROM coin.training;");
			rs = ps.executeQuery();
			while (rs.next()){
				n = rs.getInt(1);
			}
		} catch (SQLException e) {
			//new Logger().log(LogType.ERROR,  "getUrlFromWebPool()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return n;
	}
	
	/**
	 * Get the total number of feeds currently in the POOL_RSS
	 * 
	 * @return	n	Total number of RSS feeds
	 */
	public int getFeedsTotal(){
		int n = 0;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT COUNT(*) FROM coin.pool_rss;");
			rs = ps.executeQuery();
			while (rs.next()){
				n = rs.getInt(1);
			}
		} catch (SQLException e) {
			//new Logger().log(LogType.ERROR,  "getUrlFromWebPool()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return n;
	}
	
	/**
	 * Get the total number of items in QUEUE_WEB that are marked unvisited.
	 * 
	 * @return	n	Total number of RSS feeds
	 */
	public int getWebQueueTotal(){
		int n = 0;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT COUNT(*) FROM coin.queue_web WHERE visited = 0;");
			rs = ps.executeQuery();
			while (rs.next()){
				n = rs.getInt(1);
			}
		} catch (SQLException e) {
			System.out.println("DatabaseManagerge:getWebQueueTotal(). Error "+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return n;
	}
	
	/**
	 * Get the total number of trainable feeds currently in the POOL_RSS
	 * 
	 * @return	n	Total number of RSS feeds
	 */
	public int getTrainableFeedsTotal(){
		int n = 0;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT COUNT(*) FROM coin.pool_rss WHERE trainable = 1;");
			rs = ps.executeQuery();
			while (rs.next()){
				n = rs.getInt(1);
			}
		} catch (SQLException e) {
			//new Logger().log(LogType.ERROR,  "getUrlFromWebPool()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return n;
	}
	
	/**
	 * DEPRECATED. There is no web pool. Will return the oldest URL from the web pool.
	 * 
	 * @return	result	a url from the pool
	 */
	public String getFromWebPool(){
		String url = null;
		int id;
		
		try {
			connect();
			ps = conn.prepareStatement("SELECT * FROM coin.pool_web ORDER BY last_crawl ASC LIMIT 1");
			rs = ps.executeQuery();
			while (rs.next()){
				id = rs.getInt(1);
				url = rs.getString(2);
				//update 
				ps = conn.prepareStatement("UPDATE coin.pool_web SET last_crawl = now() WHERE id = ?");
				ps.setInt(1, id);
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			//new Logger().log(LogType.ERROR,  "getUrlFromWebPool()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return url;
	}

	public boolean setProcessedUrl(int id, String url) {
		System.out.println("DatabaseManager:setProcessedUrl()=url="+url);
		System.out.println("DatabaseManager:setProcessedUrl()=id="+id);
		try {
			connect();
			ps = conn.prepareStatement("UPDATE coin.processed SET url = ? WHERE id = ?");
			ps.setString(1, url);
			ps.setInt(2, id);
			ps.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("DatabaseManager:setProcessedUrl()="+e.getMessage());
		} finally {
			DatabaseUtilities.close(conn);
			DatabaseUtilities.close(ps);
			DatabaseUtilities.close(rs);
		}
		
		return true;
	}


	
	
}
