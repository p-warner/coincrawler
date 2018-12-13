package com.coin;
/**
 * 
 * ScaperWebpage class is responsible for getting data from the web, validating (check status 200 OK), 
 * and sanitizing (stopping injection attacks). This object is very self-contained and mutate oriented
 * to keep all IO items in the same thread. 
 * 
 * @author Phil
 * */

import java.net.*;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.jsoup.*;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;

import java.io.*;

public class ScraperWebpage extends Scraper{
	private ArrayList<String> links = new ArrayList<String>();
	private ArrayList<String> feeds = new ArrayList<String>();
	
	static private DatabaseManager db;
	protected String url;
	private String response;
	protected Document doc;

	
	/**
	 * Load url before running the thread. In instantiate a scraper, you must pass in a string.
	 * 
	 * @param url	URL in string format
	 */
	ScraperWebpage(String url, DatabaseManager db){
		super("scraper.webpage");
		this.db = db;
		this.url = url;
	}
	
	/**
	 * Run these in order once thread is ready to go.
	 */
	@Override
	public void run() {
		if(!isStatusOk())
			return;//thread may exit prematurely here
		getContents();
		parseContents();
		if(getContentRelevancyWeight() < 100)
			return;//thread may exit prematurely here
		insertContentIntoProcess();
		//only feed links. I waste a lot of resources finding webpages that are 'news articles'.
		//rss feeds provide all we need.
		getFeedLinks();
		insertFeedsIntoPool();
		//let rss insert links for now.
		//this.getLinks();
		//this.insertLinksIntoQueue();
		
	}

	private int getContentRelevancyWeight(){
		//item weight
		int weight = 0;
		//get keywords
		String[][] words = db.getKeywords();
		
		//in URL?
		for(int i = 0; i < words.length; i++){
			if(url.contains(words[i][0])){
				weight += Integer.parseInt(words[i][1]);
			}
		}
		
		if(weight < 99){
			Elements samples = doc.select("title,h1,p:nth-child(3)");
			for (Element sample : samples) {
				for(int i = 0; i < words.length; i++){
					if(sample.text().contains(words[i][0])){
						weight += Integer.parseInt(words[i][1]);
					}
				}
			}
		}
		
		System.out.println(super.scraperId+":isContentRelated(), Keyword weight for "+url+" is "+weight);

		return weight;
	}

	/**
	 * Parses the response variable into the doc variable. Parsing is handled by JSOUP.
	 * Also sanitizes the response with jsoup.
	 */
	protected void parseContents(){
		doc = Jsoup.parse(Jsoup.clean(response, Whitelist.relaxed()));
	}
	
	/**
	 * Makes the HTTP GET request. Loads the global response variable with the HTTP response.
	 * 
	 */
	protected void getContents(){
		StringBuilder response = new StringBuilder();
		try {
			URL url = new URL(this.url);
			URLConnection conn = url.openConnection();
			conn.setRequestProperty("Accept", "text/html");
			conn.setRequestProperty("Accept-Language", "en-us,en;q=0.5");
			conn.setRequestProperty("User-Agent", USER_AGENTS[0]); 
			
			BufferedReader in;
			in = new BufferedReader(new InputStreamReader(conn.getInputStream(), Charset.forName("UTF-8")));
			
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
	 * Checks the HTTP status response header of a URL.
	 * 
	 * @return	exit	true if OK, false if anything else
	 */
	protected boolean isStatusOk(){
		boolean isOK = true;
		try {
			URL url = new URL(this.url);
			URLConnection conn = url.openConnection();
			conn.setRequestProperty("Accept", "text/html");
			conn.setRequestProperty("Accept-Language", "en-us,en;q=0.5");
			conn.setRequestProperty("User-Agent", USER_AGENTS[0]); 
			if(conn.getHeaderField(0)==null || !conn.getHeaderField(0).contains("200 OK")){
				isOK = false;
			}
		} catch (IOException e) {
			System.out.println(super.scraperId+":isStatusOk(), "+e.getMessage());
			isOK = false;
		}
		return isOK;
	}
	
	/**
	 * Inserts the link ArrayList into the queue table for other scrapers to gather.
	 */
	private void insertLinksIntoQueue(){
		for (String link : links) {
			this.db.insertIntoWebQueue(this.url, link);
		}
		
	}
	
	/**
	 * Inserts the link ArrayList into the queue table for other scrapers to gather.
	 */
	private void insertFeedsIntoPool(){
		for (String link : feeds) {
			if(db.inRssPool(link))
				continue;
			
			this.db.insertIntoFeedPool(link);
		}
		
	}
	
	/**
	 * Inserts interesting DOM elements into the DATA column of PROCESS
	 */
	private void insertContentIntoProcess(){
		Elements content = doc.select("h1,h2,p");
		Elements pubdate = doc.select("[pubdate]");
		db.insertIntoProcess(this.url, content.toString(), "dom");
	}
	
	/**
	 * Inserts feed links into the feeds variable. Content must be parsed before this method is called.
	 */
	private void getFeedLinks(){
		//<link rel="alternate" type="application/rss+xml" title="CoinDesk RSS Feed" href="https://feeds.feedburner.com/CoinDesk"/>
		Elements feeds = doc.select("link[type*=\"rss+xml\"]");
		
		for (Element link : feeds) {
			String url = link.attr("href");
			String title = link.attr("title");
			
			//no comment feeds (popular wordpress feature.)
			if(url.contains("comments/feed"))
				continue;
			
			if(url.contains(url+"feed"))
				continue;
			
			if(title.contains("Comments")||title.contains("comments"))
				continue;
			
			this.feeds.add(url);
			
			System.out.println(super.scraperId+":getFeedLinks(), found rss="+url);
		}
	}
	
	
	
	/**
	 * Loads the link ArrayList. Performs various checks to only insert good links.
	 * 
	 */
	private void getLinks(){
		Elements links = doc.select("a[href]");
        
		for (Element link : links) {
		  String href = link.attr("href");
		  
		  //section anchors are pointless
		  if(href.charAt(0) == '#')
			  continue;
		  
		  //remove query strings and anchors
		  if(href.contains("#"))
			  href = href.substring(0, href.indexOf('#'));
		  
		  if(href.contains("?"))
			  href = href.substring(0, href.indexOf('?'));
		  
		  //no protocol? add one
		  if(href.length() > 2 && href.substring(0,2).equals("//"))
			  href = "https:" + href;
		  
		  //relative links
		  if(!href.contains("http"))
			  href = url + '/' + href;
		  
		  if(db.inWebQueue(href))
			  continue;
		  
		  //check if it's already in the ArrayList
		  if(!this.links.contains(href))
			  this.links.add(href);//add to array list
		  
		}

	}
}
