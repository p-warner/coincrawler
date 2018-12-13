package com.coin;
import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.text.Normalizer;

import opennlp.tools.*;
import opennlp.tools.doccat.*;
import opennlp.tools.sentdetect.*;
import opennlp.tools.tokenize.*;
import opennlp.tools.util.*;

/**
 * Training manager is called by the user. It will pull the latest 1000 items from COIN.PROCESSED and put them into COIN.TRAINING
 * Once the records are in COIN.TRIANING they are available for crowd-sourced voting. Next it writes a sentiment analysys training
 * document, based on the votes in the training database, in training/sentiment/en-sentiment-crypto.train. Once that document is
 * created, it is up to the user to execute the command line training tool.
 * 
 * @author Phil
 *
 */
public class TrainingManager {
	private static DatabaseManager db = new DatabaseManager();
	
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
		//Get latest processed headlines
		ArrayList<String> list = db.getProcessAggregate(1000);
		
		System.out.println(list);
		
		for(String item : list){
			System.out.println(item);
			db.insertIntoTraining(item, null);
		}
		
		//tally votes
		countVotes();
		writeSentimentTrainingDocument();
		
		//execute trainer on command line, not api
		System.out.println("Linux-");
		System.out.println("./opennlp.bat DoccatTrainer -model /c/Users/Phil/workspace/Coin/models/en-crypto-sentiment.bin -lang en -data /c/Users/Phil/workspace/Coin/training/sentiment/en-crypto-sentiment.train -encoding UTF-8");
		//LINUX
		//./opennlp.bat DoccatTrainer -model /c/Users/Phil/workspace/Coin/models/en-crypto-sentiment.bin -lang en -data /c/Users/Phil/workspace/Coin/training/sentiment/en-crypto-sentiment.train -encoding UTF-8
		
		System.out.println("Windows-");
		System.out.println("opennlp.bat DoccatTrainer -model c:/Users/Phil/workspace/Coin/models/en-crypto-sentiment11-8-2017.bin -lang en -data c:/Users/Phil/workspace/Coin/training/sentiment/en-crypto-sentiment.train -encoding UTF-8");
		//WINDOWS
		//opennlp.bat DoccatTrainer -model c:/Users/Phil/workspace/Coin/models/en-crypto-sentiment11-8-2017.bin -lang en -data c:/Users/Phil/workspace/Coin/training/sentiment/en-crypto-sentiment.train -encoding UTF-8
		
		//Training API, no working... :(
		/*DoccatModel model = null;
		MockInputStreamFactory dataIn = null;
		File trainingDocument = new File("training/sentiment/en-crypto-sentiment.train");
		try{
			
			dataIn = new MockInputStreamFactory(trainingDocument);
			ObjectStream<String> lineStream = new PlainTextByLineStream(dataIn, "UTF-8");
			ObjectStream<DocumentSample> sampleStream = new DocumentSampleStream(lineStream);
			
			TrainingParameters params = new TrainingParameters();
			DoccatFactory df = new DoccatFactory();
			
			model = DocumentCategorizerME.train("en", sampleStream, params, df);
			
			OutputStream modelOut = new BufferedOutputStream(new FileOutputStream("models/en-crypto-sentiment.new"));
					
			model.serialize(modelOut);
				
			
		}catch(Exception e){
			e.printStackTrace();
		}*/
		/**
		 * End Sentiment Analysis training
		 */
		
		/**
		 * Begin doccat training
		 */
		//scrape known crypto outlets, put into QUEUE_TRAINING
		//scrapeTrainableFeeds();
		//System.out.println("heres the fucking thing ’".indexOf('\u2019'));
		//gather items from queue_training and write them to the samples folder
		//writeTrainableItemsToFiles();
		
		//gather up samples and write into a trainable format.
		//writeTrainingDocument();

		//To generate a model-
		//in command line enter-
		//./opennlp.bat DoccatTrainer -model en-doccat.bin -lang en -data /c/Users/Phil/workspace/Coin/en-doccat.train -encoding UTF-8
		/**
		 * End doccat training
		 */
	}
	
	private static void countVotes() {
		//get everything from training table
		String[][] ti = db.getAllSentimentTrainingItemsForCounting();
		System.out.println(ti.length + " items for training.");
		//for each row, tally votes
		for(int row = 0; row < ti.length; row++){
			System.out.print(ti[row][0]+", ");
			System.out.print(ti[row][1]+", ");
			System.out.print(ti[row][2]+", ");
			System.out.print(ti[row][3]+", ");
			System.out.print(ti[row][4]+", ");
			System.out.print(ti[row][5]+", ");
			System.out.print("\n");
			
			if(ti[row][3] == null && ti[row][4] == null && ti[row][5] == null)
				continue;
			
			int pos = (ti[row][3] != null) ? Integer.parseInt(ti[row][3]) : 0 ;
			int N = (ti[row][4] != null) ? Integer.parseInt(ti[row][4]) : 0 ;
			int neg = (ti[row][5] != null) ? Integer.parseInt(ti[row][5]) : 0 ;
			
			
			if(pos == neg){
				db.updateTrainingItemPolarity(Integer.parseInt(ti[row][0]), "N");
			}else if(pos > neg){
				if(pos >= N)
					db.updateTrainingItemPolarity(Integer.parseInt(ti[row][0]), "pos");
				else
					db.updateTrainingItemPolarity(Integer.parseInt(ti[row][0]), "N");
			}else{
				if(neg >= N)
					db.updateTrainingItemPolarity(Integer.parseInt(ti[row][0]), "neg");
				else
					db.updateTrainingItemPolarity(Integer.parseInt(ti[row][0]), "N");
			}
		}
	}


	/**
	 * Scrape rss feeds marked trainable and place into the QUEUE_TRAINABLE table..
	 */
	private static void scrapeTrainableFeeds(){
		String feed;
		for(int i = 0; i < db.getTrainableFeedsTotal(); i++){
			feed = db.getTrainableFromFeedPool();
			System.out.println(feed);
			Runnable r = new ScraperRss(feed, true);
			new Thread(r).start();
		}

		
	}
	
	private static void writeSentimentTrainingDocument(){
		FileWriter fw;
		
		try {
			fw = new FileWriter("training/sentiment/en-crypto-sentiment.train", true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter writer = new PrintWriter(bw);
			
			String [][] items = db.getSentimentTrainingItems();
			
			for(int i = 0; i < items.length; i++){
				//System.out.println("Writing "+items[i][0]+".");
				writer.println(items[i][1]+"\t"+items[i][0]);
			}
			
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Scans a directory for sample data and writes it to a training file.
	 * 
	 * @throws IOException
	 */
	private static void writeTrainingDocument() throws IOException{
		String[] categories = {"crypto", "rando"};
		
		for(int i = 0; i < categories.length; i++){
			File dir = new File("training/category/"+categories[i]);
			File[] directoryListing = dir.listFiles();
			
			//open file for writing
			FileWriter fw = new FileWriter("training/category/en-doccat.train", true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter writer = new PrintWriter(bw);
			
			if (directoryListing != null) {
				for (File child : directoryListing) {
					String str = "", temp = "";

					//get raw data from file
					BufferedReader br = new BufferedReader(new FileReader(child));
					while ((temp = br.readLine()) != null) {
						str += temp;
					}
					System.out.println("TrainingManager:writeTrainingDocument(), Raw data from file "+child+"\r\n");
					
					//clean raw a little
					
					
					//break into sentences
					InputStream sentenceModelIn = new FileInputStream("models/en-sent.bin");
					SentenceModel sentenceModel = new SentenceModel(sentenceModelIn);
					SentenceDetectorME sentenceDetector = new SentenceDetectorME(sentenceModel);
					String sentences[] = sentenceDetector.sentDetect(str);
					
					//System.out.println("Sentence length="+sentences.length);
					
					writer.print(categories[i]+"\t");
					int count = 0;
					for(String sentence : sentences){
						//System.out.println("Sentence "+ (++count) +": \n"+sentence);
						//tokenize sentences
						sentence = Normalizer.normalize(sentence, Normalizer.Form.NFC);
						
						InputStream tokenizerModelIn = new FileInputStream("models/en-token.bin");
						TokenizerModel tokenModel = new TokenizerModel(tokenizerModelIn);
						Tokenizer tokenizer = new TokenizerME(tokenModel);
						String tokens[] = tokenizer.tokenize(sentence);
						
						if(count!=0)
							writer.print("\t\t");
						for(String token : tokens){
							writer.print(token + " ");
						}
						writer.println("");
						count++;
					}
					
					
				}
			} else {
				// Handle the case where dir is not really a directory.
				// Checking dir.isDirectory() above would not be sufficient
				// to avoid race conditions with another process that deletes
				// directories.
			}
			writer.close();
		}
		
	}

}
