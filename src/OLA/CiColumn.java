package OLA;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CiColumn {
	
	int iD;    // ID号
	public List<Map<String,String>> columnList = new ArrayList<Map<String,String>>();     // 这里当时设计的不好，应该直接使用Map<String,String>
	int flag = 0;   // 1表示mark
	int parent1;
	int parent2;
	
	//构造函数
	public CiColumn() {}
	
	//设置iD
	public void setiD(int _id){
		iD = _id;
	}
	
	//设置columnList
	public void setcolumnList(List<Map<String,String>> _columnList){
		columnList = _columnList;
	}
	
	//设置columnList
	public void addMap2columnList(Map<String,String> map){
		columnList.add(map);
	}
	
	//根据iD设置flag,其实就是mark操作
	public void setflag(int id){
		flag = 1;
	}
	
	public void setparent1(int _parent1){
		parent1 = _parent1;
	}
	
	public void setparent2(int _parent2){
		parent2 = _parent2;
	}
	
	//设置iD
	public int getGenLevelByAttr(String Attr){
		for (int i = 0; i < this.columnList.size(); i++) {
			Map<String,String> ms = this.columnList.get(i);
			for (Map.Entry<String,String> me : ms.entrySet()) {
				if (me.getKey().equals(Attr)) {
					return Integer.valueOf(me.getValue());
				}
			}
		}
		return -1;        // 表示错误，没有找到
	}
	
}
