package OLA;


import java.util.ArrayList;

public class FsTable {

	ArrayList<FsColumn> lfs = new ArrayList<FsColumn>();
	
	public FsTable() {};
	
	public void add(FsColumn fsc){
		lfs.add(fsc);
	}
	
	
	// ´Ó1¿ªÊ¼
	public FsColumn getFsColumn(int num){
		int i = num - 1;
		return lfs.get(i);
		
	}
	
}
