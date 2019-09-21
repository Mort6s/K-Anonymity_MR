package Incognito_SQLExpress_Census;

import java.util.ArrayList;
import java.util.List;

public class GenTable {

	List<GenColumn> lgc = new ArrayList<GenColumn>();
	
	public GenTable(){
		
	}
	
	public void addGenColumn(GenColumn gc){
		lgc.add(gc);
	}
	
	public GenColumn getGenColumn(String _attr_name){
		for(GenColumn gc:lgc){
			if(gc.attr_name.equals(_attr_name)){
				return gc;
			}
		}
		return null;
	}
	
	public void AutoCreateGenItem(){
		for(int i=0; i<lgc.size(); i++){
			lgc.get(i).AutoCreategenLevelItem();
		}
	}
	
	public List<String> GetAllAttr(){
		List<String> allAttr = new ArrayList<String>();
		for(int i=0; i<lgc.size(); i++){
			allAttr.add(lgc.get(i).attr_name);
		}
		return allAttr;
	}	
}
