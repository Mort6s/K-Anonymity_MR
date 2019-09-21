package Incognito_SQLExpress_Census;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class Incognito_SQL_IL {
	
	public static void main(String[] args) throws SQLException {  
		
		long startTime = System.currentTimeMillis();
		
		List<String> attQ = public_data.attQ;
		
		DBHelper help=new DBHelper();
        MPPHelper mh = new MPPHelper();
        GenHelper gh = new GenHelper();

        int n_attQ = attQ.size();  
		
		DataTable dataTrans = new DataTable(); 
        
        List<String> lsc = new ArrayList<String>();
        for(int i=0;i<n_attQ;i++){
        	String att_now = attQ.get(i).toString();
        	String sSQL = "select " + att_now + " from census_income";
	       	lsc = help.GetSingleColumn(sSQL);
	       	dataTrans.addColumn(att_now, lsc);
        }
         
        List<Double> list_IL = new ArrayList<Double>();
        
        String Ci = "C" + Integer.toString(n_attQ); 
        String Ei = "E" + Integer.toString(n_attQ);
        CiTable ciT = help.readRootNode2Ci(Ci, Ei);
        mh.PrintCiTable(ciT);
        int nn = ciT.lCi.size();
        DataTable dt_gen = new DataTable();
        Map<List<String>, Integer> frequencySet = new HashMap<List<String>, Integer>();
        
        for(int j = 1; j <= nn; j++) {
        	
        	CiColumn node = ciT.getCiColumn(j);
        	System.out.println("==================Now calculate no." + j + " Information Loss==================");
        	System.out.println("Gen node:");
        	mh.PrintCiColumn(node);
			dt_gen =  gh.getGenDataTable(dataTrans, node);
			dt_gen = mh.TransposeDT(dt_gen);
			
			frequencySet = mh.getFrequencySet(dt_gen);
			
			FsTable fst = mh.getFsTable(dt_gen, frequencySet);
			
			List<String> attQ_All = attQ;
			IL_Table ilt = new IL_Table();
			for(int w=1; w<=fst.lfs.size(); w++){
				FsColumn fsc = fst.getFsColumn(w);
				
				IL_Column ilc = new IL_Column();
				ilc.index = w;
				
				for (int q = 0; q < attQ_All.size(); q++){

					IL_Base ilb = new IL_Base();
					ilb.sAttrname = attQ_All.get(q);
					ilb.sClass = gh.gt.getGenColumn(ilb.sAttrname).attr_class;
					ilb.iCount = fsc.FsCount;
					if(ilb.sClass.equals("Numerical")){
							String sSQL = "select " + ilb.sAttrname + " from adult";
					       	List<String> ls = help.GetListFromColumnbyNum(sSQL, fsc.list_ID);
				            Double max = Double.parseDouble(ls.get(0));     
				            Double min = Double.parseDouble(ls.get(0)); 
				            for (int r = 0; r < ls.size(); r++) {          
				                      if (min > Double.parseDouble(ls.get(r))) min = Double.parseDouble(ls.get(r));   
				                      if (max < Double.parseDouble(ls.get(r))) max = Double.parseDouble(ls.get(r));        
				            }
				            ilb.dMax = max;
				            ilb.dMin = min;
				            ilb.dRange = gh.gt.getGenColumn(ilb.sAttrname).dRange;
						}
					else if(ilb.sClass.equals("Categorical")){
						ilb.dHSub = node.getGenLevelByAttr(ilb.sAttrname);
						ilb.dHAll = gh.gt.getGenColumn(ilb.sAttrname).maxGenLevel;
					}
					ilc.lil.add(ilb); 
				
				}
				 
				ilt.lcc.add(ilc);	
			}
			EvaluationMethod em = new EvaluationMethod();
			System.out.println("");
			System.out.println("IL = " + em.IL(ilt));
			
			list_IL.add(em.IL(ilt));
        }
        
        System.out.println("");
        mh.PrintListDouble(list_IL);

		long endTime = System.currentTimeMillis();
		long totalTime = 0;
		totalTime+= endTime - startTime; 
		System.out.println("Total time: " + totalTime + "ms");
		System.out.println("Average time: " + totalTime/list_IL.size() + "ms");
	}
}
