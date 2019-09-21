package Incognito_SQLExpress_Census;

import java.util.ArrayList;
import java.util.List;

public class DataColumn {

	
	String columnName = new String();
	List<String> columnList = new ArrayList<String>();

	public DataColumn() {}
	
	public DataColumn(String _columnName, List<String> _columnList) {
		columnName = _columnName;
		columnList = _columnList;
	}
	
	public void setColumnName(String sc){
		columnName = sc;
	}
	
	public String getColumnName(){
		return columnName;
	}
	
	public List<String> getColumnList(){
		return columnList;
	}
	
	public String getValue(int num){
		return columnList.get(num-1);	
	}
		
	public int size(){
		return columnList.size();	
	}
	
	public void add2ColumnList(String sl){
		columnList.add(sl);
	}	
}
