package Incognito_SQLExpress_Census;

import java.util.ArrayList;
import java.util.List;



public class DataTable {
	
	List<DataColumn> dataT = new ArrayList<DataColumn>();
	
	public DataTable(){}
	
	public void addColumn(DataColumn dc){
		dataT.add(dc);
	}
	
	public void editColumn(int index, DataColumn dc) {
		dataT.set(index, dc);
	}
	
	public void addColumn(String cn,List<String> cl){
		DataColumn dc = new DataColumn(cn,cl);
		dataT.add(dc);
	}
	
	public DataColumn getColumn(String cn){
		for(DataColumn dc: dataT){
			if(dc.columnName.equals(cn)){
				return dc;
			}			
		}
		return null;
	}
	
	public DataColumn getColumn(int num){
		int i = 0;
		for(DataColumn dc: dataT){
			i ++;
			if(i == num){
				return dc;
			}
		}
		return null;
	}
	
	public int getColumnCount(){
		return dataT.size();		
	}	
}
