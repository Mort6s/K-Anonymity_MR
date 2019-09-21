package Incognito_SQLExpress_Census;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Paper_AL_MR {

	public static void main(String[] args) throws Exception {
		int k = public_data.k;
		List<String> attQ = public_data.attQ;

		String tableName = "census_income";
        Integer totalNum = public_data.totalNum;
		double dSamPer = public_data.dSamPer;
		Integer AllCount = public_data.AllCount;
		
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
        	String sSQL = "select top " + AllCount + " " + att_now + " from " + tableName;
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
		int totalMaxAttrLevel = gh.getMaxAttrLevelbyAttrName("age") + gh.getMaxAttrLevelbyAttrName("class_of_worker") + gh.getMaxAttrLevelbyAttrName("education");
		List<List<Integer>> llBestPath = new ArrayList<List<Integer>>();
		
		while(seed <= totalMaxAttrLevel) {			
			Map<List<Integer>, Double> mGenGroupPlusOne = new HashMap<List<Integer>, Double>();
			
			// 1 "age"
			if(genLevel_age < gh.getMaxAttrLevelbyAttrName("age")) {
				List<Integer> lGenGroup = Arrays.asList(new Integer[]{genLevel_age + 1, genLevel_class_of_worker, genLevel_education, genLevel_marital_status, genLevel_race, genLevel_sex, genLevel_country_of_birth_self});
				
				double dIL_GenGroup = mh.getILbyGenGroup_M(dataTrans_Sample, lGenGroup, attQ, dataTrans);
				
				mGenGroupPlusOne.put(lGenGroup, dIL_GenGroup);
			}
			// 3 "class_of_worker"
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
			// 8 "country_of_birth_self"
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
		
		long frequencyTime = 0;
		// (3.1) -- (3.7)
		while(dataTrans.dataT.get(0).columnList.size() > k && index <= llBestPath.size()) {
			 
			System.out.println("==================The current is progressing No." + index + " times calculation.==================");
			System.out.println("The count of rows is " + dataTrans.dataT.size());
			System.out.println("The count of columns is " + dataTrans.dataT.get(0).columnList.size());
			List<Integer> lGenGroup_Now = llBestPath.get(index - 1);

			long stime = System.currentTimeMillis(); 
			DataTable dt_gen =  gh.getGenDataTable(dataTrans, lGenGroup_Now);
			long etime = System.currentTimeMillis();
			System.out.println("time getGenDataTable:"+String.valueOf(etime-stime)+"ms");

			stime = System.currentTimeMillis(); 
			dt_gen = mh.TransposeDT(dt_gen);
			etime = System.currentTimeMillis();
			System.out.println("time dt_gen:"+String.valueOf(etime-stime)+"ms");

			long frequencyStartTime = System.currentTimeMillis();
			System.out.println("Calculating frequent items sets...");
			
			FsTable fst_Satisfy_k = null;
			if(dt_gen.getColumnCount() >= public_data.MR_Condition_Size)
			{
				GetFrequencySet gs = new GetFrequencySet();
				Map<List<String>, String> frequencySet = gs.GetFrequencySet(dt_gen, public_data.otherArgs);
				System.out.println("Calculating frequency sets...");
				fst_Satisfy_k = mh.getFsTable_MRAndSatisfyK(frequencySet);
			}
			else
			{
				Map<List<String>, Integer> frequencySet = mh.getFrequencySet(dt_gen);
				System.out.println("Calculating frequency sets...");
				Map<List<String>, Integer> frequencySetSatisfyK = mh.getFrequencySetSatisfyK(frequencySet, k);
				fst_Satisfy_k = mh.getFsTable(dt_gen, frequencySetSatisfyK);
			}
			long frequencyEndTime = System.currentTimeMillis();
			frequencyTime += frequencyEndTime - frequencyStartTime;
			long time5 = System.currentTimeMillis(); 
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
							String sSQL = "select " + ilb.sAttrname + " from " + tableName;
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
			long time6 = System.currentTimeMillis(); 
			long IL1Time = 0;
			IL1Time += time6 - time5;
			System.out.println("IL use time:" + IL1Time + "ms");
			
			EvaluationMethod em = new EvaluationMethod();
			
			long time7 = System.currentTimeMillis(); 
			double IL_ilt = em.IL(ilt);
			totalIL = totalIL + IL_ilt;
			System.out.println("");
			System.out.println("Information Loss in this round: IL = " + IL_ilt);

			long time8 = System.currentTimeMillis(); 
			long IL2Time = 0;
			IL2Time += time8 - time7;
			System.out.println("IL use time:" + IL2Time + "ms");
			
			stime = System.currentTimeMillis();
			dataTrans = mh.delListIDInDataTable2(dataTrans, fst_Satisfy_k);
			etime = System.currentTimeMillis();
			System.out.println("delete list ID in table use time: "+ String.valueOf(etime-stime) + "ms");
			
			index = index + 1;
		}
		
		System.out.println("Total information loss: IL = " + totalIL);
		
		long endTime = System.currentTimeMillis();
		long totalTime = 0;
		totalTime+= endTime - startTime;
		long phase3Time = 0;
		phase3Time+= endTime - pathTime;
		System.out.println("FrequencyTime: "+frequencyTime+"ms");
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
