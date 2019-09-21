package OLA;

import java.util.ArrayList;
import java.util.List;

public class GenTable {

	List<GenColumn> lgc = new ArrayList<GenColumn>();
	
	// 构造函数
	public GenTable(){
		
	}
	
	// 添加规则
	public void addGenColumn(GenColumn gc){
		lgc.add(gc);
	}
	
	// 获得规则
	public GenColumn getGenColumn(String _attr_name){
		for(GenColumn gc:lgc){
			if(gc.attr_name.equals(_attr_name)){
				return gc;
			}
		}
		return null;
	}
	
	// 自动生成泛化规则
	public void AutoCreateGenItem(){
		for(int i=0; i<lgc.size(); i++){
			lgc.get(i).AutoCreategenLevelItem();
		}
	}
	
	
	// 获得属性集合
	public List<String> GetAllAttr(){
		List<String> allAttr = new ArrayList<String>();
		for(int i=0; i<lgc.size(); i++){
			allAttr.add(lgc.get(i).attr_name);
		}
		return allAttr;
	}
	
}
