package Incognito_SQLExpress_Census;

import java.util.List;

public class GenAttrLevel {

	String attr;
	List<Integer> level_Item;
	
	public GenAttrLevel(){};
	
	public void setAttr(String _attr){
		attr = _attr;
	}
	
	public void setLevel_Item(List<Integer> _level_Item){
		level_Item = _level_Item;
	}
}
