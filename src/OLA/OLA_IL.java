package OLA;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class OLA_IL {


	
	public static void main(String[] args) throws SQLException {  
		
		// ��¼��ʼʱ��
		long startTime = System.currentTimeMillis(); // ���ÿ�ʼʱ��
		
	    // ���������ݽ��и�ֵ
		List<String> attQ = public_data.attQ;
		
		// ��ʼ��������
		DBHelper help=new DBHelper();
        MPPHelper mh = new MPPHelper();
        GenHelper gh = new GenHelper();  // ����������

        // n_attQ��ʾ׼��ʶ�����Եĸ���
        int n_attQ = attQ.size();  
		
		// ��ʼ�����ݱ�dataTrans
		DataTable dataTrans = new DataTable(); 
        
        
       // ����attQ��׼��ʶ�����ԣ���ȡ���ݵ�dataTrans��
       List<String> lsc = new ArrayList<String>();
        for(int i=0;i<n_attQ;i++){
        	String att_now = attQ.get(i).toString();
//	       	String sSQL = "select top 5000 " + att_now + " from adult";
        	String sSQL = "select " + att_now + " from census_income";
	       	lsc = help.GetSingleColumn(sSQL);
	       	dataTrans.addColumn(att_now, lsc);
       }
         
        
        // ����һ��List��������ÿ������k�ķ�����ϼ������ILֵ
        List<Double> list_IL = new ArrayList<Double>();
        
       
        // (���岿��) �����Ǽ���IL
        // (3) ����ɢ��QI�����������㷨������ݶ�ȡ��ciT
        String Ci = "C" + Integer.toString(n_attQ); 
        CiTable ciT = help.readSql2Ci(Ci);
        mh.PrintCiTable(ciT);
//        int nn = 1;
        int nn = ciT.lCi.size();
        DataTable dt_gen = new DataTable();
        Map<List<String>, Integer> frequencySet = new HashMap<List<String>, Integer>();
        // (4) �����������ֵ�����Խ����������㷨���õ����п��ܵ�Ci����ѭ��������
        for(int j = 1; j <= nn; j++) {
        	
        	// (4.1) ��÷��������Եķ�������
        	CiColumn node = ciT.getCiColumn(j);      // ��÷�������
        	System.out.println("==================��ǰΪ��" + j + "����ѡk�������Ե�ILֵ���㣡==================");
        	System.out.println("��ѡk�����������Ϊ:");
        	mh.PrintCiColumn(node);
			dt_gen =  gh.getGenDataTable(dataTrans, node);          // ��÷�������
			dt_gen = mh.TransposeDT(dt_gen);           // ����������ת��
			// mh.PrintTable(dt_gen);
			
			// (4.2) ����������Ե�Ƶ���
			frequencySet = mh.getFrequencySet(dt_gen);   // ���Ƶ���
			// mh.PrintFrequencySet(frequencySet);
			// System.out.println(frequencySet.size());
			
			// (4.3) ����(4.1)��(4.2)�Ľṹ����Ƶ�ʼ���
			FsTable fst = mh.getFsTable(dt_gen, frequencySet);
			// System.out.println(fst.lfs.get(0).list_ID.size());
			// ˵����Ƶ�����ÿһ�ж������˷���Ƶ�����Ӧ����ֵ���Ե�ID��
			
			
			// (4.8) ����IL_Table
			List<String> attQ_All = attQ;
			IL_Table ilt = new IL_Table();
			for(int w=1; w<=fst.lfs.size(); w++){
				FsColumn fsc = fst.getFsColumn(w);   // �����������µ�Ƶ���
				
				IL_Column ilc = new IL_Column();
				ilc.index = w;
				
				for (int q = 0; q < attQ_All.size(); q++){

					IL_Base ilb = new IL_Base();
					ilb.sAttrname = attQ_All.get(q);
					ilb.sClass = gh.gt.getGenColumn(ilb.sAttrname).attr_class;
					ilb.iCount = fsc.FsCount;
					if(ilb.sClass.equals("Numerical")){
							String sSQL = "select " + ilb.sAttrname + " from adult";
					       	List<String> ls = help.GetListFromColumnbyNum(sSQL, fsc.list_ID);     // ע��lli.get(p)��ʵ���ǰ���Num�б��Ƶ���
				            Double max = Double.parseDouble(ls.get(0));     
				            Double min = Double.parseDouble(ls.get(0)); 
				            for (int r = 0; r < ls.size(); r++) {          
				                      if (min > Double.parseDouble(ls.get(r))) min = Double.parseDouble(ls.get(r));   
				                      if (max < Double.parseDouble(ls.get(r))) max = Double.parseDouble(ls.get(r));        
				            }
				            ilb.dMax = max;
				            ilb.dMin = min;
//							ilb.dMin = 0;
//							ilb.dMax = gh.gtd.getAttrRangeInGenLevel(ilb.sAttrname, node.getGenLevelByAttr(ilb.sAttrname));
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
			
			// ����ǰILֵ��ӵ�List��
			list_IL.add(em.IL(ilt));
        }
        
        // ��Console�������ʾ
        System.out.println("");
        mh.PrintListDouble(list_IL);

		long endTime = System.currentTimeMillis(); // ���ý���ʱ��
		long totalTime = 0;
		totalTime+= endTime - startTime; // ��ʱ��
		System.out.println("����ʱ��" + totalTime + "ms");
		System.out.println("ƽ����ʱ��" + totalTime/list_IL.size() + "ms");
	}
        
        
	
	
	
	
	
	
	
	
	
	

	
	
}
