package Incognito_SQLExpress_Census;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Paper_AL_NOMR {
		
	public static void main(String[] args) {
		int k = public_data.k;
		List<String> attQ = public_data.attQ;

		String tableName = "census_income";
        Integer totalNum = 95130;
		double dSamPer = 0.01;
		
		long startTime = System.currentTimeMillis();		
		
		DBHelper help=new DBHelper();
        MPPHelper mh = new MPPHelper();
        GenHelper gh = new GenHelper();
        
        System.out.println("The current access to the first step: the original data sampling.");
		List<Integer> lIndex = getSysSampling(totalNum, dSamPer);
		System.out.println("The number of the samples is:");
		mh.PrintListInteger(lIndex);
		
		DataTable dataTrans = new DataTable(); 
		DataTable dataTrans_Sample = new DataTable();
		
        int n_attQ = attQ.size();  
		
	    List<String> lsc = new ArrayList<String>();
	    	for(int i=0;i<n_attQ;i++){
	        String att_now = attQ.get(i).toString();
	        String sSQL = "select " + att_now + " from " + tableName;
		    lsc = help.GetSingleColumn(sSQL);
		    dataTrans.addColumn(att_now, lsc);
	    }
	    DataTable dataTrans_DT = mh.TransposeDT(dataTrans); 
	    
	    for(int i = 0; i < lIndex.size(); i ++) {
	    	dataTrans_Sample.addColumn(dataTrans_DT.getColumn(lIndex.get(i)));
	    }
	     
	    System.out.println("The current access to the second step: get the optimal generalization path.");
		dataTrans_Sample = mh.TransposeDT(dataTrans_Sample);
		for (int i = 0; i < attQ.size(); i++){
			dataTrans_Sample.dataT.get(i).columnName = attQ.get(i);
		}

		int genLevel_age = 0;                // 1
		int genLevel_class_of_worker = 0;    // 3
		int genLevel_education = 0;          // 4
		int genLevel_marital_status = 0;     // 5
		int genLevel_race = 0;               // 6
		int genLevel_sex = 0;                // 7
		int genLevel_country_of_birth_self = 0;     // 8
		int seed = 1;
		int totalMaxAttrLevel = gh.getMaxAttrLevelbyAttrName("age") + gh.getMaxAttrLevelbyAttrName("class_of_worker") + gh.getMaxAttrLevelbyAttrName("education") + gh.getMaxAttrLevelbyAttrName("marital_status") + gh.getMaxAttrLevelbyAttrName("race") + gh.getMaxAttrLevelbyAttrName("sex") + gh.getMaxAttrLevelbyAttrName("country_of_birth_self");
		List<List<Integer>> llBestPath = new ArrayList<List<Integer>>();
		
		while(seed <= totalMaxAttrLevel) {
			
			Map<List<Integer>, Double> mGenGroupPlusOne = new HashMap<List<Integer>, Double>();
			// 1 "age"
			if(genLevel_age < gh.getMaxAttrLevelbyAttrName("age")) {
				List<Integer> lGenGroup = Arrays.asList(new Integer[]{genLevel_age + 1, genLevel_class_of_worker, genLevel_education, genLevel_marital_status, genLevel_race, genLevel_sex, genLevel_country_of_birth_self});
				
				double dIL_GenGroup = mh.getILbyGenGroup_M(dataTrans_Sample, lGenGroup, attQ, dataTrans);
				
				mGenGroupPlusOne.put(lGenGroup, dIL_GenGroup);
			}
			// 3 "workclass"
			if(genLevel_class_of_worker < gh.getMaxAttrLevelbyAttrName("class_of_worker")) {
				List<Integer> lGenGroup = Arrays.asList(new Integer[]{genLevel_age, genLevel_class_of_worker + 1, genLevel_education, genLevel_marital_status, genLevel_race, genLevel_sex, genLevel_country_of_birth_self});
				
				double dIL_GenGroup = mh.getILbyGenGroup_M(dataTrans_Sample, lGenGroup, attQ, dataTrans);
				
				mGenGroupPlusOne.put(lGenGroup, dIL_GenGroup);
			}
			// 4 "education"
			if(genLevel_education < gh.getMaxAttrLevelbyAttrName("education")) {
				List<Integer> lGenGroup = Arrays.asList(new Integer[]{genLevel_age, genLevel_class_of_worker, genLevel_education + 1, genLevel_marital_status, genLevel_race, genLevel_sex, genLevel_country_of_birth_self});
				
				double dIL_GenGroup = mh.getILbyGenGroup_M(dataTrans_Sample, lGenGroup, attQ, dataTrans);
				
				mGenGroupPlusOne.put(lGenGroup, dIL_GenGroup);
			}
			// 5 "marital_status"
			if(genLevel_marital_status < gh.getMaxAttrLevelbyAttrName("marital_status")) {
				List<Integer> lGenGroup = Arrays.asList(new Integer[]{genLevel_age, genLevel_class_of_worker, genLevel_education, genLevel_marital_status + 1, genLevel_race, genLevel_sex, genLevel_country_of_birth_self});
				
				double dIL_GenGroup = mh.getILbyGenGroup_M(dataTrans_Sample, lGenGroup, attQ, dataTrans);
				
				mGenGroupPlusOne.put(lGenGroup, dIL_GenGroup);
			}
			// 6 "race"
			if(genLevel_race < gh.getMaxAttrLevelbyAttrName("race")) {
				List<Integer> lGenGroup = Arrays.asList(new Integer[]{genLevel_age, genLevel_class_of_worker, genLevel_education, genLevel_marital_status, genLevel_race + 1, genLevel_sex, genLevel_country_of_birth_self});
				
				double dIL_GenGroup = mh.getILbyGenGroup_M(dataTrans_Sample, lGenGroup, attQ, dataTrans);
				
				mGenGroupPlusOne.put(lGenGroup, dIL_GenGroup);
			}
			// 7 "sex"
			if(genLevel_sex < gh.getMaxAttrLevelbyAttrName("sex")) {
				List<Integer> lGenGroup = Arrays.asList(new Integer[]{genLevel_age, genLevel_class_of_worker, genLevel_education, genLevel_marital_status, genLevel_race, genLevel_sex + 1, genLevel_country_of_birth_self});
				
				double dIL_GenGroup = mh.getILbyGenGroup_M(dataTrans_Sample, lGenGroup, attQ, dataTrans);
				
				mGenGroupPlusOne.put(lGenGroup, dIL_GenGroup);
			}
			// 8 "native_country"
			if(genLevel_country_of_birth_self < gh.getMaxAttrLevelbyAttrName("country_of_birth_self")) {
				List<Integer> lGenGroup = Arrays.asList(new Integer[]{genLevel_age, genLevel_class_of_worker, genLevel_education, genLevel_marital_status, genLevel_race, genLevel_sex, genLevel_country_of_birth_self + 1});
				
				double dIL_GenGroup = mh.getILbyGenGroup_M(dataTrans_Sample, lGenGroup, attQ, dataTrans);
				
				mGenGroupPlusOne.put(lGenGroup, dIL_GenGroup);
			}
			
			double min = 10000000000000.00;
			List<Integer> li_min = new ArrayList<Integer>();
			for (Map.Entry<List<Integer>, Double> me : mGenGroupPlusOne.entrySet()) {
				if(me.getValue() < min) {
					min = me.getValue();
					li_min = me.getKey();
				}
			}
			genLevel_age = li_min.get(0);                // 1
			genLevel_class_of_worker = li_min.get(1);    // 3
			genLevel_education = li_min.get(2);          // 4
			genLevel_marital_status = li_min.get(3);     // 5
			genLevel_race = li_min.get(4);               // 6
			genLevel_sex = li_min.get(5);                // 7
			genLevel_country_of_birth_self = li_min.get(6);     // 8			
			
			llBestPath.add(li_min);			
			
			seed = seed + 1;
		}
		
		System.out.println("The optimal generalization path is: ");
		for(int j = 0; j < llBestPath.size(); j++) {
			mh.PrintListInteger(llBestPath.get(j));
		}
		
		long pathTime = System.currentTimeMillis();
		long phase2Time = 0;
		phase2Time+= pathTime - startTime;
		
		
		int index = 1;
		double totalIL = 0.0;
		
		while(dataTrans.dataT.get(0).columnList.size() > k && index <= llBestPath.size()) {
			System.out.println("==================The current is progressing No." + index + " times calculation.==================");
			System.out.println("The count of rows is " + dataTrans.dataT.size());
			System.out.println("The coutn of columns is " + dataTrans.dataT.get(0).columnList.size());
			List<Integer> lGenGroup_Now = llBestPath.get(index - 1);
			
			DataTable dt_gen =  gh.getGenDataTable(dataTrans, lGenGroup_Now);
			dt_gen = mh.TransposeDT(dt_gen); 
			System.out.println("Calculating frequent items sets...");
			Map<List<String>, Integer> frequencySet = mh.getFrequencySet(dt_gen);
			System.out.println("Calculating frequency sets...");
			Map<List<String>, Integer> frequencySetSatisfyK = mh.getFrequencySetSatisfyK(frequencySet, k);
			FsTable fst_Satisfy_k = mh.getFsTable(dt_gen, frequencySetSatisfyK);
			
			IL_Table ilt = new IL_Table();
			for(int w=1; w<=fst_Satisfy_k.lfs.size(); w++){
				
				FsColumn fsc = fst_Satisfy_k.getFsColumn(w);
				IL_Column ilc = new IL_Column();
				ilc.index = w;
				for (int q = 0; q < attQ.size(); q++){
					
					IL_Base ilb = new IL_Base();    
					ilb.sAttrname = attQ.get(q); 
					ilb.sClass = gh.gt.getGenColumn(ilb.sAttrname).attr_class;
					ilb.iCount = fsc.FsCount;
					if(ilb.sClass.equals("Numerical")) {
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
						ilb.dHSub = lGenGroup_Now.get(q);
						ilb.dHAll = gh.gt.getGenColumn(ilb.sAttrname).maxGenLevel;
					}
					ilc.lil.add(ilb); 				
				}				 
				ilt.lcc.add(ilc);	
			}
			EvaluationMethod em = new EvaluationMethod();
			double IL_ilt = em.IL(ilt);
			totalIL = totalIL + IL_ilt;
			System.out.println("");
			System.out.println("Information Loss in this round: IL = " + IL_ilt);
			dataTrans = mh.delListIDInDataTable2(dataTrans, fst_Satisfy_k);
			index = index + 1;
		}
		
		System.out.println("Total information loss: IL = " + totalIL);
		
		long endTime = System.currentTimeMillis();
		long totalTime = 0;
		totalTime+= endTime - startTime;
		long phase3Time = 0;
		phase3Time+= endTime - pathTime;
		System.out.println("Path: " + phase2Time + "ms");
		System.out.println("IL: " + phase3Time + "ms");
		System.out.println("Total time: " + totalTime + "ms");
	}
	
	public static List<Integer> getSysSampling(Integer totalNum, double dSamPer) {		
	
		int iCount = (int) Math.floor(totalNum * dSamPer);
		int iInteval = (int) Math.floor(totalNum / iCount);
		
		List<Integer> result = new ArrayList<Integer>();
		int index = 1;
		while(index < totalNum) {
			result.add(index);
			index = index + iInteval;
		}
		return result;		
	}	
}
