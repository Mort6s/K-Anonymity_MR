package OLA;


import java.util.ArrayList;
import java.util.List;

public class FsColumn {

	List<String> Item = new ArrayList<String>();     // 等价类属性集合
	ArrayList<Integer> list_ID = new ArrayList<Integer>();     // 该等价类对应的ID形成的List
	int FsCount = 0;          // 等价类中元组个数
	DataTable dt = new DataTable();    // 等价类元组构成的table,dt的顺序和list_ID一致
	double[][] centers;    //中心点的值
	List<List<Integer>> sub_list_ID = new ArrayList<List<Integer>>();  // 根据centers形成的list
	
	
	public FsColumn() {};
	
	
	
}
