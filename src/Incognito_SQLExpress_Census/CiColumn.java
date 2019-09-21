package Incognito_SQLExpress_Census;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CiColumn {
	
	int iD;
	List<Map<String,String>> columnList = new ArrayList<Map<String,String>>();
	int flag = 0;
	int parent1;
	int parent2;

	public CiColumn() {}

	public void setiD(int _id){
		iD = _id;
	}
	
	public void setcolumnList(List<Map<String,String>> _columnList){
		columnList = _columnList;
	}
	
	public void addMap2columnList(Map<String,String> map){
		columnList.add(map);
	}
	
	public void setflag(int id){
		flag = 1;
	}
	
	public void setparent1(int _parent1){
		parent1 = _parent1;
	}
	
	public void setparent2(int _parent2){
		parent2 = _parent2;
	}
	
	public int getGenLevelByAttr(String Attr){
		for (int i = 0; i < this.columnList.size(); i++) {
			Map<String,String> ms = this.columnList.get(i);
			for (Map.Entry<String,String> me : ms.entrySet()) {
				if (me.getKey().equals(Attr)) {
					return Integer.valueOf(me.getValue());
				}
			}
		}
		return -1;
	}
	
}
