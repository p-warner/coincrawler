package com.coin;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

/**
 * Scrapes an RSS feed from COIN.POOL_RSS and will place urls into the 
 * COIN.QUEUE_WEB table or QUEUE_TRAINABLE table if isTrainable is true.
 * 
 * @author Phil
 *
 */
public class ScraperRss extends Scraper{
	private static DatabaseManager db = new DatabaseManager();
	private ArrayList<String> links = new ArrayList<String>();
	private String feed, response;
	private Document doc;
	private boolean isTraining;
	
	/**
	 * Load thread up with a feed.
	 * 
	 * @param feed	The feed url in string format
	 */
	ScraperRss(String feed, boolean isTraining) {
		super("scraper.rss");//logging id
		this.feed = feed;
		this.isTraining = isTraining;
	}

	/**
	 * Run these in order once thread is ready to go.
	 */
	@Override
	public void run() {
		getStatus();
		getContents();
		parseContents();
		getLinks();
		insertLinksIntoQueue();
	}
	
	/**
	 * Checks the HTTP status (200 OK) response header of a FEED. Exits thread if not 200 OK.
	 * 
	 */
	private void getStatus(){
		//System.out.println(super.scraperId+":getStatus(), "+feed);
		try {
			URL url = new URL(feed);
			URLConnection conn = url.openConnection();
			if(conn.getHeaderField(0)==null || !conn.getHeaderField(0).contains("200 OK")){
				System.out.println(super.scraperId+":getStatus(), not OK");
			}
		} catch (IOException e) {
			System.out.println(super.scraperId+":getStatus(), "+e.getMessage());
		}
	}
	
	/**
	 * Loads the global response variable with the http response.
	 * 
	 */
	private void getContents(){
		//System.out.println(super.scraperId+":getContents()");
		StringBuilder response = new StringBuilder();
		try {
			URL url = new URL(feed);
			URLConnection conn = url.openConnection();
			conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
			conn.setRequestProperty("Accept-Language", "en-us,en;q=0.5");
			conn.setRequestProperty("User-Agent", USER_AGENTS[0]); 
			
			BufferedReader in;
			in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			
			String temp;
			while((temp = in.readLine()) != null){
				response.append(temp);
			}
			
			in.close();
		} catch (IOException e) {
			System.out.println(super.scraperId+":getContents(), "+e.getMessage());
		}
		this.response = response.toString();
	}
	
	/**
	 * Parses the response variable into the doc variable.
	 */
	private void parseContents(){
		doc = Jsoup.parse(response, "", Parser.xmlParser());
	}
	
	/**
	 * Inserts the link ArrayList into the queue table for other scrapers to gather.
	 */
	private void insertLinksIntoQueue(){
		for (String link : links) {
			if(isTraining)
				db.insertIntoTrainingQueue(feed, link, "crypto");
			else
				db.insertIntoWebQueue(feed, link);
			
			//System.out.println(super.scraperId+":insertIntoQueue("+link+")");
		}
		
	}
	
	/**
	 * Loads the link ArrayList. Performs various checks to only insert good links.
	 * 
	 */
	private void getLinks(){
		Elements links = doc.select("item link");
        
		for (Element link : links) {
		  String href = link.text();
		  System.out.println("link="+href); 
		  
		  if(isTraining){
			  if(db.inTrainingQueue(href))
				  continue;
		  }else{
			 if(db.inWebQueue(href))
			  continue; 
		  }

		  //check if it's already in the ArrayList
		  if(!this.links.contains(href))
			  this.links.add(href);//add to array list
		  
		}

	}
}
