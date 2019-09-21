package OLA;


public class GenBase {

	int gen_level;    // 泛化级别
	String original_value;   // 原始属性值，用于分类型属性
	int start_num;    // 开始范围，用于数值型属性
	int end_num;     // 结束范围，用于数值型属性。
	String gen_value;  // 泛化后的属性值
	
	//构造函数,用于分类型属性
	public GenBase(int _gen_level,String _original_value,String _gen_value){
		gen_level = _gen_level;
		original_value = _original_value;
		gen_value = _gen_value;
		
	}
	
	//构造函数,用于数值型属性
	public GenBase(int _gen_level,int _start_num,int _end_num,String _gen_value){
		gen_level = _gen_level;
		start_num = _start_num;
		end_num = _end_num;
		gen_value = _gen_value;
		
	}
	
	
	
	
}
