package com.coin;
/**
 * Abstract class for scraper that all scrapers must inherit from.
 * 
 * @author Phil
 *
 */

public abstract class Scraper implements Runnable {
	final static String[] USER_AGENTS = {
				"CoinCrawler/.2",
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36"
				}; 
	final String scraperId;
	//protected static DatabaseManager db = new DatabaseManager();
	
	Scraper(String scraperId){
		this.scraperId = scraperId;
	}
}
