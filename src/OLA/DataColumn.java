package OLA;

import java.util.ArrayList;
import java.util.List;



public class DataColumn {

	
	String columnName = new String();
	List<String> columnList = new ArrayList<String>();

	
	// 构造函数
	public DataColumn() {}
	
	// 构造函数
	public DataColumn(String _columnName, List<String> _columnList) {
		columnName = _columnName;
		columnList = _columnList;
	}
	
	// 设置ColumnName
	public void setColumnName(String sc){
		columnName = sc;
	}
	
	// 获得ColumnName
	public String getColumnName(){
		return columnName;
	}
	
	// 获得columnList
	public List<String> getColumnList(){
		return columnList;
	}
	
	
	// 根据num获得属性值，num从1开始编号
	public String getValue(int num){
		return columnList.get(num-1);	
	}
	
	
	public int size(){
		return columnList.size();	
	}
	
	// 将某个string添加到columnList属性中
	public void add2ColumnList(String sl){
		columnList.add(sl);
	}
	
	

    
	
	
}
