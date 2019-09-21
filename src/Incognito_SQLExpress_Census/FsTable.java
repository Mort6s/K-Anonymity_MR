package Incognito_SQLExpress_Census;


import java.util.ArrayList;

public class FsTable {

	ArrayList<FsColumn> lfs = new ArrayList<FsColumn>();
	
	public FsTable() {};
	
	public void add(FsColumn fsc){
		lfs.add(fsc);
	}
	
	public FsColumn getFsColumn(int num){
		int i = num - 1;
		return lfs.get(i);		
	}	
}
