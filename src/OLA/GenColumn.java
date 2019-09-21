package OLA;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class GenColumn {
	
	String attr_name;    // 属性名
	String attr_class;   // 属性类型，如果为"Categorical"表示分类型属性，如果为"Numerical"表示数值型属性
	List<GenBase> lgb = new ArrayList<GenBase>();
	List<Integer> genLevelItem = new ArrayList<Integer>();   // 在AutoCreategenLevelItem()方法中初始化
	int maxGenLevel;
	double dRange;    // 用于数值型属性，表示范围
	
	// 构造函数
	public GenColumn(){
	}
	
	// 设置属性名
	public void setAttr_name(String _attr_name){
		attr_name = _attr_name;
	}
	
	// 设置属性类别
	public void setAttr_class(String _attr_class){
		attr_class = _attr_class;
	}
	
	// 添加泛化规则：分类型属性
	public void addGenRule(int _gen_level,String _original_value,String _gen_value){
		GenBase gb = new GenBase(_gen_level,_original_value,_gen_value);
		lgb.add(gb);
	}
	
	// 添加泛化规则：数值型属性
	public void addGenRule(int _gen_level,int _start_num,int _end_num,String _gen_value){
		GenBase gb = new GenBase(_gen_level,_start_num,_end_num,_gen_value);
		lgb.add(gb);
		
	}
	
	// 删除所有Rule
	public void delAllGenRule(){
		Iterator<GenBase> it = lgb.iterator();
		while(it.hasNext()){
		  it.remove();
		}
	}
	
	// 获得泛化级别的列表
	public List<Integer> getLevelItem(){
		List<Integer> li = new ArrayList<Integer>();
		for(int i=0; i<lgb.size(); i++){
			if(!li.contains(lgb.get(i).gen_level)){   //查看新集合中是否有指定的元素，如果没有则加入
                li.add(lgb.get(i).gen_level);
            }
		}
		return li;
	}
	
	public void AutoCreategenLevelItem(){
		for(GenBase gb:lgb){
			if(!genLevelItem.contains(gb.gen_level)){
				genLevelItem.add(gb.gen_level);
			}	
		}
		maxGenLevel = Collections.max(genLevelItem);     // 这一步很关键，获得最大的泛化级别
	}
	
	
}
