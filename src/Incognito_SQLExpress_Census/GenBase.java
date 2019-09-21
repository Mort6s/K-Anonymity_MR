package Incognito_SQLExpress_Census;


public class GenBase {

	int gen_level;
	String original_value;
	int start_num;
	int end_num;
	String gen_value;
	
	public GenBase(int _gen_level,String _original_value,String _gen_value){
		gen_level = _gen_level;
		original_value = _original_value;
		gen_value = _gen_value;		
	}
	
	public GenBase(int _gen_level,int _start_num,int _end_num,String _gen_value){
		gen_level = _gen_level;
		start_num = _start_num;
		end_num = _end_num;
		gen_value = _gen_value;		
	}
}
