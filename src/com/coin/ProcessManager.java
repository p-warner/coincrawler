package com.coin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import opennlp.tools.doccat.DoccatModel;
import opennlp.tools.doccat.DocumentCategorizerME;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

/**
 * Provides an entry point for processing records in the PROCESS table and puts them into the COIN.PROCESSED table.
 * This manager will run a sentiment analysis on all unprocessed items in COIN.PROCESS. It will also run ProcessAggregate
 * once.
 * 
 * @author Phil
 *
 */
public class ProcessManager {
	private static DatabaseManager db = new DatabaseManager();
	public static void main(String[] args) throws IOException {

		processRemainingItems();
		
		//ProcessAggregate trending = new ProcessAggregate();
		Runnable r = new ProcessAggregate();
		r.run();
		
		//pull last 48hrs of items totals
		System.out.println("\nThe last 2 days there are:");
		String[][] totals = db.getPredictionTotals();
		for(int i = 0; i < 3; i++){
			System.out.println(totals[i][0] + " " + totals[i][1]);
		}
		generatePrediction(totals);
		
	}
	
	/**
	 * Accepts a 2d array of predictions in a specific order. It takes a simple majority and will return 
	 * up, down or steady.
	 * 
	 * @param totals
	 */
	private static void generatePrediction(String[][] totals) {
		// Simple majority now... something better later?
		String prediction = "steady";
		int neutral = Integer.parseInt(totals[0][1]), 
				negative = Integer.parseInt(totals[1][1]), 
				positive = Integer.parseInt(totals[2][1]);
		
		if(positive > negative){
			if(positive*2 > neutral)
				prediction = "up";
		}else{
			if(negative*2 > neutral)
				prediction = "down";
		}
		
		//place into DB
		db.insertIntoPrediction(prediction, "{\"N\":"+totals[0][1]+",\"neg\":"+totals[1][1]+",\"pos\":"+totals[2][1]+"}");
		
	}

	/**
	 * Accepts a regular String in sentence format. Returns the polarity of a
	 * sentence in the cryptocurrency space. We must have up-to-date models! See 
	 * TrainingManager to create a new model.
	 * 
	 * @param	in	Sentence in String format
	 * @return	polarity	polarity of the sentence
	 */
	private static String getSentiment(String in){
		String polarity = null;
		try{
			InputStream modelIn = new FileInputStream("models/en-token.bin");
			TokenizerModel model = new TokenizerModel(modelIn);
			Tokenizer tokenizer = new TokenizerME(model);
			String tokens[] = tokenizer.tokenize(in);
			
			InputStream is = new FileInputStream("models/en-crypto-sentiment.bin");
			DoccatModel m = new DoccatModel(is);
			DocumentCategorizerME myCategorizer = new DocumentCategorizerME(m);
			
			double[] outcomes = myCategorizer.categorize(tokens);
			//These are the percentage values for each category
			//for(double n : outcomes)
				//System.out.println(n);
		polarity = myCategorizer.getBestCategory(outcomes);
		
		}catch(Exception e){
			System.out.println("ProcessManager:getSentimentAnalysis(), exception in NLP.");
			e.printStackTrace();
		}
		return polarity;
	}
	
	/**
	 * Takes article and ID runs sentiment analysis on it, then pushes the SA results into
	 * the database, then marks the record as processed in PROCESS.
	 * 
	 * @param article	array with row ID as index 0, and text as index 1
	 */
	private static void processSingleItem(String[] article){
		if(article[1] != null){
			Document doc = Jsoup.parse(article[1]);
			String title = doc.select("h1").text().replaceAll("[^A-Za-z0-9\\s]", "");
		
			if(title.length() > 0){
				String polarity = getSentiment(title);
				db.setSentimentForId(Integer.parseInt(article[0]), polarity, title, article[2]);
				System.out.println("Insert into processed: ("+polarity+") "+article[0]+ ", " +title+", "+article[2]);
			}
		}
		db.setProcessed(Integer.parseInt(article[0]));
		//db.setProcessedUrl(Integer.parseInt(article[0]), article[2]);
	}
	
	/**
	 * Runs through unprocessed records.
	 */
	private static void processRemainingItems(){
		String[][] items = db.getAllUnprocessed();
		
		System.out.println("Processing " + items.length + " items.");
		for(int i = 0; i < items.length; i++){
			processSingleItem(items[i]);
		}
	}
}
