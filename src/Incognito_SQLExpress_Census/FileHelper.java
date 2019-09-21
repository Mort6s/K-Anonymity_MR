package Incognito_SQLExpress_Census;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class FileHelper {
	// 1. Function: Read Text Data Set
	public List<List<String>> readTrans(String fileDir,String splitFlag) {
			// TODO Auto-generated method stub
		List<List<String>> records = new ArrayList<List<String>>();
		BufferedReader br;
		try {
			FileReader fr = new FileReader(new File(fileDir));
			br = new BufferedReader(fr);

			String line = null;
			while ((line = br.readLine()) != null) {
				if (line.trim() != "") {
					List<String> record = new ArrayList<String>();
					String[] items = line.split(splitFlag);
					for (String item : items) {
						record.add(item);
					}
					records.add(record);
				}
			}
		} catch (IOException e) {
			System.out.println("Read File Failed");
			System.exit(-2);
		}
		return records;
	}
	public DataTable readTransToDataTable(String fileDir,String splitFlag, List<String> attQ) {

		List<String> attQAll = public_data.attQAll;
		List<Integer> attQI = new ArrayList<Integer>();
		
		Hashtable<String, Integer> ht = new Hashtable<String, Integer>();
		for(int i = 0; i < attQAll.size(); i++)
		{
			ht.put(attQAll.get(i), i);
		}
		for(int i = 0; i < attQ.size(); i++)
		{
			Integer a = ht.get(attQ.get(i));
			attQI.add(a);
		}
		
		// TODO Auto-generated method stub
		DataTable records = new DataTable();
		BufferedReader br;
		try {
			FileReader fr = new FileReader(new File(fileDir));
			br = new BufferedReader(fr);

			String line = null;
			int count = 1;
			while ((line = br.readLine()) != null) {
				if (line.trim() != "") {
					List<String> record = new ArrayList<String>();
					String[] items = line.split(splitFlag);
					for(int i = 0; i < attQI.size(); i++)
					{
						record.add(items[attQI.get(i)]);
					}
					records.addColumn(Integer.toString(count), record);
					count++;
				}
			}
		} catch (IOException e) {
			System.out.println("Read File Failed");
			System.exit(-2);
		}
		return records;
	}	
	
	// 2.Function: Output text file.
	public void wrData(List<List<String>> dataTrans, FileWriter tgFileWriter,String wrFlag) throws IOException {
		
		for (List<String> ds : dataTrans) {
			int i = 0;
			for (String d : ds) {
				i = i + 1 ;
				if(i < ds.size())
				{
					tgFileWriter.append(d + wrFlag);
				}
				else
				{
					tgFileWriter.append(d);
				}
				
			}
			tgFileWriter.append("\n");

		}
		tgFileWriter.flush();
	}	
	
	// 3.Function: Output text file by DataTable
	public void wrData(DataTable dataTrans, FileWriter tgFileWriter,String wrFlag) throws IOException {
		
		for (DataColumn ds : dataTrans.dataT) {
			List<String> ls = ds.columnList;
			int i = 0;
			for (String d : ls) {
				i = i + 1 ;
				if(i < ls.size()){
					tgFileWriter.append(d + wrFlag);
				}else{
					tgFileWriter.append(d);
				}				
			}
			tgFileWriter.append("\n");
		}
		tgFileWriter.flush();
	}
}
