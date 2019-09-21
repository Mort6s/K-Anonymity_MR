package Incognito_SQLExpress_Census;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Incognito_SQL {

	
	public static void main(String[] args) throws SQLException {  
		
		long startTime = System.currentTimeMillis();
		
		int k = public_data.k;
		List<String> attQ = public_data.attQ;
		
		DBHelper help=new DBHelper();
        MPPHelper mh = new MPPHelper();
        GenHelper gh = new GenHelper();
		
        int n_attQ = attQ.size();  
		
		DataTable dataTrans = new DataTable(); 
          
        List<String> lsc = new ArrayList<String>();
        for(int i = 0; i < n_attQ; i++) {
        	String att_now = attQ.get(i).toString();
        	String sSQL = "select " + att_now + " from census_income";
	       	lsc = help.GetSingleColumn(sSQL);
	       	dataTrans.addColumn(att_now, lsc);
        }
        
        Main_Preparation_Work mpw = new Main_Preparation_Work();
        mpw.initTable();
        
        CiTable queue = new CiTable();
        int n = attQ.size();
        for(int i = 1; i <= n; i++) {
        	System.out.println("==================Now calculate no." + i + " attribute==================");
        	
        	String Ci = "C" + Integer.toString(i);
        	String Ei = "E" + Integer.toString(i);
        	CiTable ciT = help.readSql2Ci(Ci);
        	EiTable eiT = help.readSql2Ei(Ei);
        	
        	List<Integer> lint = new ArrayList<Integer>();
        	for(int j = 1; j <= eiT.getEiColumncount(); j++) {
        		int flag = 1;
        		for(int k1 = 1; k1 <= eiT.getEiColumncount(); k1++) {
        			if(eiT.getEiColumn(j).EiStart == eiT.getEiColumn(k1).EiEnd){
        				flag = 0;
        			}
        		}
        		if(flag ==1){
        			lint.add(eiT.getEiColumn(j).EiStart);
        		}
        	}
        	List<Integer> lint2 = new ArrayList<Integer>();
        	CiTable root_queue = new CiTable();
            for (int j = 0; j < lint.size(); j++){
                if(!lint2.contains(lint.get(j))){
                	lint2.add(lint.get(j));
                }
            }
        	for(int j = 0; j < lint2.size(); j++){
        		queue.addCiColumn(ciT.getCiColumn(lint2.get(j)));
        		root_queue.addCiColumn(ciT.getCiColumn(lint2.get(j)));
        	}
        	while(queue.getCiColumncount() != 0){
        		// (5) Remove first item from queue
        		CiColumn node = queue.getCiColumn(1);
        		queue.removeColumn(1);
        		
        		Map<List<String>, Integer> frequencySet = new HashMap<List<String>, Integer>();
        		if(node.flag == 0){     // if node is not marked then
        			DataTable dt_gen = new DataTable();
        			if(root_queue.isContain(node)){   // if node is a root
        				dt_gen =  gh.getGenDataTable(dataTrans, node);
        				dt_gen = mh.TransposeDT(dt_gen);
        				frequencySet = mh.getFrequencySet(dt_gen);
        			}else{
        				dt_gen =  gh.getGenDataTable(dataTrans, node);   
        				dt_gen = mh.TransposeDT(dt_gen);
        				frequencySet = mh.getFrequencySet(dt_gen);
        			}
        			if(mh.isk(frequencySet, k)){
        				mh.markAllDirectGenNode(node, ciT, eiT);
        			}else{
        				// (1) Delete node from Ci
        				ciT.removeColumnByAttrName(node.iD);
        				
        				// (2) Insert direct generalizations of node into queue
        				List<Integer> nodenow = new ArrayList<Integer>();
        				nodenow.add(node.iD);
        				List<Integer> directGenNode = eiT.getDirectGenNode(nodenow);
        				for(int z=0; z< directGenNode.size(); z++){
        					if (ciT.getCiColumnByID(directGenNode.get(z))!=null){
        					queue.addCiColumn(ciT.getCiColumnByID(directGenNode.get(z)));
        					}
        				}
        			}
        		}
        	}
        	System.out.println("");
        	if(i == n){
            	help.insert2Ci(ciT, i);
            	mh.PrintCiTable(ciT);
        	}else{
            	help.insert2Ci(ciT, i);
            	help.insert2NextCi(i+1);
            	help.insert2NextEi(i+1);
            	System.out.println("test");
        	}
        }
		long endTime = System.currentTimeMillis();
		long totalTime = 0;
		totalTime+= endTime - startTime;
		System.out.println("Incognito total time: " + totalTime + "ms");
	}
}
