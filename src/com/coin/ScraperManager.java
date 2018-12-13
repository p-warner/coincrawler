/**
 * Manages data gathering for processing. This file gets executed by the OS as needed. 
 * This manager will spawn WebScraper threads for each row in COIN.QUEUE_WEB. Each WebScraper
 * thread will make an http request to the URL and save the response in COIN.PROCESS.
 * 
 * @author Phil
 *
 */
package com.coin;	

import java.util.concurrent.TimeUnit;

public class ScraperManager {
	
	private static DatabaseManager db = new DatabaseManager();
	
	public static void main(String[] args) throws InterruptedException{
		System.out.println("ScraperManager:main(), Starting ScraperManager.");
		System.out.println( db.getWebQueueTotal() );
        
		//Scrape RSS feeds
		scrapeFeeds();
		//Scrape web pages
		scrapeWebpages(db.getWebQueueTotal());	
		
		System.out.println("Done.");
	}
	
	/**
	 * Pick oldest RSS item from POOL_RSS and loads the links in the feed to QUEUE_WEB table first.
	 * 
	 * @param	threads	Number of feeds to scrape
	 * @throws InterruptedException 
	 */
	private static void scrapeFeeds(int n) throws InterruptedException{
		String feed;
		for(int i = 0; i < n; i++){
			feed = db.getFromFeedPool();
			//System.out.println("ScraperManager:main(), Init new thread for scraping feed "+feed);
			Runnable r = new ScraperRss(feed, false);
			new Thread(r).start();
			
			TimeUnit.SECONDS.sleep(1);//not too many connections at once	
		}

	}
	
	/**
	 * Scrape all feeds.
	 * @throws InterruptedException 
	 */
	private static void scrapeFeeds() throws InterruptedException{
		scrapeFeeds(db.getFeedsTotal());
	}
	
	/**
	 * Pick out webpage and run ScraperWebpage on it. 
	 * 
	 * @param n	How many items in the queue to run?
	 * @throws InterruptedException
	 */
	private static void scrapeWebpages(int n) throws InterruptedException{
		String url;
		for(int i = 0; i < n; i++){
			url = db.getFromWebQueue();
			
			if(url == null)
				continue;

			System.out.println("ScraperManager:main(), Init new thread for scraping url "+url);
			
			Runnable r = new ScraperWebpage(url, db);
			new Thread(r).start();
			
			TimeUnit.SECONDS.sleep(1);//not too many connections at once	
		}
		
		
	}
}
