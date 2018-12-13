package com.coin;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Will pull out trending terms from news stories.
 * 
 * @author Phil
 *
 */
public class ProcessAggregate implements Runnable {
	private static DatabaseManager db = new DatabaseManager();
	
	@Override
	public void run() {
		try {
			main(null);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		System.out.println("ProcessAggregate:main(), Starting ProcessAggregate.");
		
		int days = 2;//num days to process
		
		ArrayList<String> trending = new ArrayList<String>(10);
		ArrayList<String>[] lists = new ArrayList[days];
		HashMap<String, Integer>[] maps = new HashMap[days];
		
		int[] wc = new int[days];
		
		for(int i = 0; i < days; i++){
			lists[i] = db.getDaysAgoProcessAggregate(i);
			maps[i] = new HashMap<String, Integer>();
			wc[i] = 0;
			
			String[] temp_words = null;
			ArrayList<String> temp_list = lists[i];
			
			for(String item : temp_list){
				temp_words = normalize(item).split(" ");
				wc[i] += temp_words.length;
				
				//put into map
				for(String el : temp_words){
					if(maps[i].get(el) == null){
						maps[i].put(el, 1);
					}else{
						maps[i].replace(el, maps[i].get(el) + 1);
					}
				}
			}
		}
		
		String[] lookup_arr = getTodaysWords().toArray(new String[10]);
		
		double ma = 0, prev_ma = 0, pct = 0, prev_pct = 0;
		
		for(int j = 0; j < lookup_arr.length; j++){
			String lookup = lookup_arr[j];
			ma = 0;
			pct = 0;
			
			if(lookup == null)
				continue;
			
			System.out.println(j+" "+lookup);
			for(int i = days-1; i > -1; i--){
				
				if(isCommon(lookup))
					continue;
				
				//System.out.println("---\nTotal word count for "+i+" days ago: " + wc[i] + " in " + lists[i].size() + " articles.");
				if(maps[i].get(lookup) == null)
					continue;
				
				if(maps[i].get(lookup) < 3)
					continue;
				
				//System.out.println("Fequency of '"+lookup+"' for "+i+" days ago: " + maps[i].get(lookup));
				prev_pct = pct;
				pct = ((double)maps[i].get(lookup) / (double)wc[i]);
				
				if((pct-prev_pct)*100 > .1){//mentions threshold... .1 at 10% increase in mentions will trigger a 'trend'
					trending.add(lookup);
				}
				
				//System.out.println("Frequency percentage of '"+lookup+"' for "+i+" days ago: "+ pct*100 + "%. [previous days pct = "+prev_pct*100+", difference today - previous = "+(pct-prev_pct)*100+"]" );
				//System.out.println("Moving average for '"+lookup+"' on day "+i+": " + ma*100);
				//System.out.println("\t\tma("+ma*100+") - prev_ma("+prev_ma*100+") = " + ((ma*100) - (prev_ma*100)));
			}
		}
		
		System.out.println(trending);

		String json = "{\"terms\" : \""+trending+"\"}";

		db.insertIntoProcessTrending(json);
		
		/*
		Comparator<Entry<String, Integer>> valueComparator = new Comparator<Entry<String,Integer>>() { 
		@Override 
			public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) { 
				Integer v1 = e1.getValue(); 
				Integer v2 = e2.getValue(); 
				return v1 - v2; 
			} 
		};
		
		//Get a set.
		Set<Entry<String, Integer>> entries = maps[0].entrySet();
		
		// Sort method needs a List, so let's first convert Set to List in Java
		List<Entry<String, Integer>> listOfEntries = new ArrayList<Entry<String, Integer>>(entries);
		
		// sorting HashMap by values using comparator 
		Collections.sort(listOfEntries, valueComparator); 
		LinkedHashMap<String, Integer> sortedByValue = new LinkedHashMap<String, Integer>(listOfEntries.size());
		
		// copying entries from List to Map 
		for(Entry<String, Integer> entry : listOfEntries){ 
			sortedByValue.put(entry.getKey(), entry.getValue()); 
		} 
		
		//System.out.println("HashMap after sorting entries by values "); 
		Set<Entry<String, Integer>> entrySetSortedByValue = sortedByValue.entrySet(); 
		
		PrintWriter writer = new PrintWriter("aggregate_words.txt", "UTF-8");
		String s;
		for(Entry<String, Integer> mapping : entrySetSortedByValue){
			s = mapping.getKey() + " ==> " + mapping.getValue();
			//System.out.println(s);
			writer.println(s);
		}
		writer.close();
		*/
		

	}

	/**
	 * Utility function that returns if the argument is a common word or not.
	 * TODO: this isn't up to snuff, we need a much larger dictionary of common words. 
	 * 
	 * @param lookup	word to lookup
	 * @return	boolean true if common
	 */
	private static boolean isCommon(String lookup) {
		String[] common = {"while","bitcoin", "cryptocurrencies","cryptocurrency", "", "\n", "could", "such", "even", "website", "market","2017","said"," could"," such"," even"," website"," market"," cryptocurrency"," platform"," ico"," tokens"," bitcoin"," users"," blockchain"," network"," future"," technology"," value"," financial"," digital"," read"," fintech"," money", "wallet","the","of","and","to","a","in","for","is","on","that","by","this","with","i","you","it","not","or","be","are","from","at","as","your","all","have","new","more","an","was","we","will","home","can","us","about","if","page","my","has","search","free","but","our","one","other","do","no","information","time","they","site","he","up","may","what","which","their","news","out","use","any","there","see","only","so","his","when","contact","here","business","who","web","also","now","help","get","pm","view","online","c","e","first","am","been","would","how","were","me","s","services","some","these","click","its","like","service","x","than","find","price","date","back","top","people","had","list","name","just","over","state","year","day","into","email","two","health","n","world","re","next","used","go","b","work","last","most","products","music","buy","data","make","them"};
		for(int i = 0; i < common.length; i++){
			if(lookup.toLowerCase().equals(common[i]))
				return true;
		}
		return false;
	}

	/**
	 * Will return an arraylist of all words found today.
	 * 
	 * @return	word_list	list items
	 */
	private static ArrayList<String> getTodaysWords(){
		String[] temp_words;
		ArrayList<String> list = new ArrayList<String>();
		ArrayList<String> word_list = new ArrayList<String>();
		list = db.getDaysAgoProcessAggregate(0);
		
		for(String item : list){
			temp_words = normalize(item).split(" ");
			
			for(String word : temp_words){
				if(!word_list.contains(word)){
					word_list.add(word);
				}
			}
		}
		
		return word_list;
	}
	
	/**
	 * Accepts string and returns a string of only alphanumeric characters.
	 * 
	 * @param str	string to clean
	 * @return	str	a cleaned string
	 */
	private static String normalize(String str){
		
		str = str.toLowerCase();
		str = str.replaceAll("</?(.*?)>", " ");//Dump HTML tags
		str = str.replaceAll("&(.*?);", " ");//Dump HTML entities
		str = str.replaceAll("[^A-Za-z0-9\\s]", "");//Dump anything not alphanumeric
		
		return str;
	}
}
