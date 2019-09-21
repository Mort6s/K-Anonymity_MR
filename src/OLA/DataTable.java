package OLA;

import java.util.ArrayList;
import java.util.List;



public class DataTable {
	
	List<DataColumn> dataT = new ArrayList<DataColumn>();
	
	// 构造函数
	public DataTable(){}
	
	// 方法：添加列
	public void addColumn(DataColumn dc){
		dataT.add(dc);
	}
	
	// 方法：添加列
	public void addColumn(String cn,List<String> cl){
		DataColumn dc = new DataColumn(cn,cl);
		dataT.add(dc);
	}
	
	// 方法：根据属性名返回列
	public DataColumn getColumn(String cn){
		for(DataColumn dc: dataT){
			if(dc.columnName.equals(cn)){
				return dc;
			}
			
		}
		return null;
	}
	
	// 方法：根据num返回列，num从1开始
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
	
	// 方法：获得Column的个数
	public int getColumnCount(){
		return dataT.size();
		
	}
	
}
