package Incognito_SQLExpress_Census;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class GetFrequencySet
{
    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(LongWritable key, Text value,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException
        {
            String line = value.toString();
            
            word.set(line);
            output.collect(word, one);
        }
    }
    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException
        {
            int sum = 0;
            while (values.hasNext())
            {
                sum += values.next().get();
            }
            //if(sum >= 50)
            output.collect(key, new IntWritable(sum));
        }
    }
    
    public static class MapWithID extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, Text>
	{
		private Text word = new Text();
		private Text IDandValue = new Text();
		
		public void map(LongWritable key, Text value,
		        OutputCollector<Text, Text> output, Reporter reporter)
		        throws IOException
		{
		    String line = value.toString();
		    
		    // Input:
		    // ID:age,address,...
		    // Output:
		    // K	age,address,...
		    // V	ID:1
		    
		    //handle with the listID
		    String listID = line.split(":")[0];
		    
		    word.set(line.split(":")[1]);
		    IDandValue.set(listID + ":1");
		    output.collect(word, IDandValue);
		}
	}
	public static class ReduceWithID extends MapReduceBase implements
	    Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterator<Text> iDandValues,
		        OutputCollector<Text, Text> output, Reporter reporter)
		        throws IOException
		{
			// Input:
		    // K	age,address,...
		    // V	ID1,ID2,ID3,...:N
			// Output is the same
			
			String listID = "";
		    int sum = 0;
		    while (iDandValues.hasNext())
		    {
		    	String valueStr = iDandValues.next().toString();
	    		listID += valueStr.split(":")[0] + ",";	//get the id of the list
		    	String value = valueStr.split(":")[1];	//get and calculate the sum of the count
		    	int count = Integer.parseInt(value);
		        sum += count;
		    }
		    listID = listID.substring(0, listID.length() - 1);	//remove the last ','
		    //if(sum >= 50)
		    output.collect(key, new Text(listID + ":" + String.valueOf(sum)));
		}
	}

    public JobConf FrequencySetJob(String inputFolder, String outputFolder)
    {
        JobConf jconf = new JobConf(GetFrequencySet.class);
        jconf.setJobName("GetFrequencySet"); 
        jconf.setOutputKeyClass(Text.class);
        jconf.setOutputValueClass(Text.class);
        jconf.setMapperClass(MapWithID.class);
        jconf.setCombinerClass(ReduceWithID.class);
        jconf.setReducerClass(ReduceWithID.class);
        jconf.setInputFormat(TextInputFormat.class);
        jconf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jconf, new Path(inputFolder));
        FileOutputFormat.setOutputPath(jconf, new Path(outputFolder));
    	
        return jconf;
    }
    
    /*
     * args[0]: input file folder
     * args[1]: output file folder
     */
    public HashMap<List<String>, String> GetFrequencySet(DataTable dt, String[] args)
    		throws Exception
    {
    	HashMap<List<String>, String> hm = null;

		HadoopHelper.HadoopConfig();
		
		long startTime = System.currentTimeMillis();
		String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

		String inputFolder = args[0] + "/" + time;			//set input file folder
		String outputFolder = args[1] + "/" + time;	// set output file folder name
		
		String inputUrl = inputFolder + "/" + time + ".txt";	//put datatable data to input hdfs
		String outputUrl = outputFolder + "/part-00000";			//get HashMap data from output hdfs
		
//		HadoopHelper.Datatable2Hdfs(dt, inputUrl, ",");	//create file in hdfs
		HadoopHelper.Datatable2HdfsWithID(dt, inputUrl, ",");	//create file in hdfs
		
		JobConf jconf = FrequencySetJob(inputFolder, outputFolder);	//set the job
		JobClient.runJob(jconf);        		//run the job
    	
		hm = HadoopHelper.ReduceOut2MapWithID(outputUrl);
		
		long endTime = System.currentTimeMillis(); //end time
		long totalTime = 0;
		totalTime += endTime - startTime; //total time
		System.out.println("getting frequency set uses total time:" + totalTime + "ms");
		
    	return hm;
    }
}