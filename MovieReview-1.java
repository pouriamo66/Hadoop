import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.io.BufferedReader;
import java.net.URI;
import java.io.FileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Student ID: 2644995
 *
 *
/*/


public class MovieReview 
{
	public static class TokenizerMapper	extends Mapper<Object, Text, Text, Text> //Setup function under TokenizerMapper is the firststep and  function of the hadoop when want to  run to toknenize the text to the word
		{
			static Set<String> SetWordsForbidden = new HashSet<String>(); 
			protected void setup(Context context) throws IOException, InterruptedException
				{	
					String FirstLine;
					if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) 
						{
							Configuration conf = context.getConfiguration();    //conf is made to be able store  text as the context
							URI[] cacheFiles = context.getCacheFiles();     
							String[] fn=cacheFiles[0].toString().split("#");    
							BufferedReader br = new BufferedReader(new FileReader(fn[1])); 
							FirstLine = br.readLine();     
							//context.write(new Text(), new Text(txt));
							//Words are converted to uppercase and sent to array
							String Words[] = FirstLine.split(",");   
							List<String> ListWords = new ArrayList<String>(); 
							ListWords = Arrays.asList(Words);     
							for(String word : ListWords)
								SetWordsForbidden.add(word.toUpperCase());       
						}
				}
            //map function proccess
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
				{	
					String[] review = value.toString().split("\t");//getiing review rate as string 
					
					String strRate = review[1];
					Integer Rate = Integer.parseInt(strRate);  //Converts the string to integer 
					
					String strReview = review[0];
					strReview = strReview.toUpperCase();// converted to upper case
					

					StringTokenizer itr = new StringTokenizer(strReview);
					while(itr.hasMoreTokens())//Reads txt file line by line
						{
							String txt = itr.nextToken();
							txt = txt.toUpperCase();//words changed to upper case
							txt = txt.replaceAll("[^a-zA-Z ]", "").trim();//regex used for ignoring the words of exclude.txt file
							txt = txt.replaceAll("'S","");
							if(!SetWordsForbidden.contains(txt) && !txt.equals("") && txt.length()>1)
								context.write(new Text(Rate.toString()), new Text(txt));//transfer the key & value to the reduce
						}
				} 
	} 

// public static class IntSumCombiner extends Reducer<Text,Text,Text,Text>
// {	
	// public void reduce(Text key, Iterable<Text> WordsArrays,Context context) throws IOException, InterruptedException
		// {
			// String strWords = "";
			// for (Text word: WordsArrays)
				// {
					// strWords = strWords + " " + word.toString();
				// }
			// context.write(key, new Text(strWords));
		// } 
// }

public static class IntSumReducer extends Reducer<Text,Text,Text,Text>
{	
	public void reduce(Text key, Iterable<Text> WordsArrays,Context context) throws IOException, InterruptedException
		{
			String strRate  = key.toString();
			HashMap<String, Integer> Mymap=new HashMap();//HashMap is used to gather all the similar words into them
			for(Text word:WordsArrays)//send the WordArrays to the word to check the similarity
				{
					String Myword=word.toString();
					if(!Mymap.containsKey(Myword))//Checks wheather the Myword consist of the key
						Mymap.put(Myword,1);
					 else
						Mymap.put(Myword,Mymap.get(Myword)+1); //counter increments when the similar word matches
				}
			
			
			String BestWord= "";
			Integer MaxCounter=0;
			for(HashMap.Entry<String, Integer> Entry : Mymap.entrySet())  //for loop find the similar words
				{
					String Word=Entry.getKey();
					Integer Counter=Entry.getValue();
					if (Counter>MaxCounter)//checks the counter of similar words
						{
							BestWord = Word;
							MaxCounter = Counter;
						}
				}
			String strOutPut = "Best Wotrd of Review " + strRate;
			strOutPut +=  " is " ;
			strOutPut +=  BestWord;
			context.write(new Text(strOutPut),new Text(MaxCounter.toString() + " Times" ));		
		} 
}

public static void main(String[] args) 
	{
		try
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Assignment");
			job.setJarByClass(MovieReview.class);               //Predefined method setJarByClass calls the Wordcount class file
			job.setMapperClass(TokenizerMapper.class);			//Predefined method setMapperClass calls Tokenizermapper class
			// job.setCombinerClass(IntSumCombiner.class);		//Predefined method setReducerClass calls IntSumReducer class
			job.setReducerClass(IntSumReducer.class);			
			job.setOutputKeyClass(Text.class);                      // OutputkeyClass is Text
			job.setOutputValueClass(Text.class);			         // OutputValueClass is Text
			FileInputFormat.addInputPath(job, new Path(args[0]));       //Inputpath is rateReviews.txt
			FileOutputFormat.setOutputPath(job, new Path(args[1]));	  //Outputpath is output folder 
			job.addCacheFile(new URI("project/exclude.txt#exclude.txt"));	//exclude words from map context		
			System.exit(job.waitForCompletion(true) ? 0 : 1);			//waitForCompletion waits for the job to be submitted and returns true is successfully submitted
		}
		catch(Exception e)
		{
			System.out.println("Check Your Main Function");
		}
	}
}