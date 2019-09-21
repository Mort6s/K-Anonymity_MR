package Incognito_SQLExpress_Census;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HadoopHelper {
		
	public static Configuration HadoopConfig()
    {
		Configuration conf = new Configuration(true);
        conf.set("fs.default.name", "hdfs://master:9000");
        conf.set("hadoop.job.user", "hadoop");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.jobtracker.address", "master:9001");
        conf.set("yarn.resourcemanager.hostname", "master");
        conf.set("yarn.resourcemanager.admin.address", "master:8033");
        conf.set("yarn.resourcemanager.address", "master:8032");
        conf.set("yarn.resourcemanager.resource-tracker.address", "master:8036");
        conf.set("yarn.resourcemanager.scheduler.address", "master:8030");
        return conf;
    }
	
	public static void Datatable2Hdfs(DataTable dt, String outUrl, String wrFlag)
			throws Exception
	{
		OutputStream out = null;
		Configuration conf = HadoopConfig();
		
        try {
        	//create file and get target file info
            FileSystem fs = FileSystem.get(URI.create(outUrl), conf);
            out = fs.create(new Path(outUrl), new Progressable() {
                @Override
                public void progress() { }});
            
            //create file end.put data begin
        	StringBuilder sb = new StringBuilder();
            for(DataColumn ds:dt.dataT){
            	List<String> ls = ds.columnList;
    			int i = 0;
    			for (String d : ls) {
    				i = i + 1 ;
    				if(i < ls.size())
    				{
    					sb.append(d + wrFlag);
    				}
    				else
    				{
    					sb.append(d);
    				}    				
    			}
    			sb.append("\n");
            }
            out.write(sb.toString().getBytes());	//put data end
        } finally {
            //File Write Complete
            IOUtils.closeStream(out);
        }
	}
	
	public static void Datatable2HdfsWithID(DataTable dt, String outUrl, String wrFlag)
			throws Exception
	{
		OutputStream out = null;
		Configuration conf = HadoopConfig();
		
        try {
        	//create file and get target file info
            FileSystem fs = FileSystem.get(URI.create(outUrl), conf);
            out = fs.create(new Path(outUrl), new Progressable() {
                @Override
                public void progress() { }});
            
	            //create file end.put data begin
	        	StringBuilder sb = new StringBuilder();
	            for(DataColumn ds:dt.dataT) {
	            	List<String> ls = ds.columnList;
	    			int i = 0;
	    			sb.append(ds.columnName + ":");
	    			for (String d : ls) {
	    				i = i + 1 ;
	    				if(i < ls.size())
	    				{
	    					sb.append(d + wrFlag);
	    				}
	    				else
	    				{
	    					sb.append(d);
	    				}    				
	    			}
	    			sb.append("\n");
	            }
	            out.write(sb.toString().getBytes());	//put data end
        } finally {
            //File Write Complete
            IOUtils.closeStream(out);
        }
	}
	
	public static HashMap<List<String>, String> ReduceOut2MapWithID(String sourceUrl)
			throws Exception
	{
		HashMap<List<String>, String> hm = new HashMap<List<String>, String>();

		FSDataInputStream fin = null;
		BufferedReader in = null;
		String line;
		Configuration conf = HadoopConfig();
        
		try{
			FileSystem fs = FileSystem.get(URI.create(sourceUrl),conf);
			fin = fs.open(new Path(sourceUrl));	//read end			
			in = new BufferedReader(new InputStreamReader(fin,"UTF-8"));
			
			//start put data to <list<string>,integer>
			while((line = in.readLine()) != null)
			{
				String[] li = line.split("\t");
				String listStr = li[0];
				String IDListWithValue = li[1];
				if(li.length == 2)
				{
					List<String> ls = new ArrayList<String>();
					char[] chs = new char[listStr.length()];
					chs = listStr.toCharArray();
					String tempBuffer = "";
					Boolean waitFlag = false;
					for(int i = 0; i < listStr.length(); i++)
					{
						switch(chs[i])
						{
							case ',':
								if(waitFlag == false)
								{
									ls.add(tempBuffer);
									tempBuffer = "";
								}
								else
								{
									tempBuffer += chs[i];
								}
								break;
							case '[':
								waitFlag = true;
								tempBuffer += chs[i];
								break;
							case ']':
								waitFlag = false;
								tempBuffer += chs[i];
								break;
							default:
								tempBuffer += chs[i];
								break;
						}
					}
					if(!tempBuffer.equals(""))
						ls.add(tempBuffer);
					hm.put(ls, IDListWithValue);
				}
			}
		} finally {
			IOUtils.closeStream(in);	//list put complete
		}
		return hm;
	}
	
	public static HashMap<List<String>, Integer> ReduceOut2Map(String sourceUrl)
			throws Exception
	{
		HashMap<List<String>, Integer> hm = new HashMap<List<String>, Integer>();
		
		FSDataInputStream fin = null;
		BufferedReader in = null;
		String line;
		Configuration conf = HadoopConfig();
        
		try{
			FileSystem fs = FileSystem.get(URI.create(sourceUrl),conf);
			fin = fs.open(new Path(sourceUrl));	//read end			
			in = new BufferedReader(new InputStreamReader(fin,"UTF-8"));
			
			//start put data to <list<string>,integer>
			while((line = in.readLine()) != null)
			{
				Integer count = null;
				String[] li = line.split("\t");
				String listStr = li[0];
				if(li.length == 2)
				{
					List<String> ls = new ArrayList<String>();
					char[] chs = new char[listStr.length()];
					chs = listStr.toCharArray();
					String tempBuffer = "";
					Boolean waitFlag = false;
					for(int i = 0; i < listStr.length(); i++)
					{
						switch(chs[i])
						{
							case ',':
								if(waitFlag == false)
								{
									ls.add(tempBuffer);
									tempBuffer = "";
								}
								else
								{
									tempBuffer += chs[i];
								}
								break;
							case '[':
								waitFlag = true;
								tempBuffer += chs[i];
								break;
							case ']':
								waitFlag = false;
								tempBuffer += chs[i];
								break;
							default:
								tempBuffer += chs[i];
								break;
						}
					}
					if(!tempBuffer.equals(""))
						ls.add(tempBuffer);
					count = Integer.valueOf(li[1]);
					hm.put(ls, count);
				}
			}
		} finally {
			IOUtils.closeStream(in);	//list put complete
		}
		
		return hm;
	}
}
