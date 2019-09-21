package OLA;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class MPPHelper {

	//构造函数 
	public MPPHelper(){}
	
	// 在Console中输出List<String>
    public void PrintListString(List<String> ls) { 
        for(int i=0; i<ls.size(); i++){
        	System.out.print(" " + ls.get(i));
        }
        System.out.println("");
    } 
	
	
	// 在Console中输出List<Integer>
    public void PrintListInteger(List<Integer> li) { 
        for(int i=0; i<li.size(); i++){
        	System.out.print(" " + li.get(i));
        }
        System.out.println("");
    } 
    
	// 在Console中输出List<Double>
    public void PrintListDouble(List<Double> li) { 
        for(int i=0; i<li.size(); i++){
        	System.out.print(" " + li.get(i));
        }
        System.out.println("");
    } 
    
	// 在Console中输出列 
    public void PrintColumn(DataColumn dc) { 
    	System.out.print(dc.columnName);    
        for(String ss : dc.columnList){
        	System.out.print(" " + ss);
        }
    } 
    
	// 在Console中输出表
    public void PrintTable(DataTable dt) { 
    	for(int i=1; i<=dt.getColumnCount(); i++){
    		PrintColumn(dt.getColumn(i));
    		System.out.println("");
    	}
    } 
    
    
    // DataTable的转置
    public DataTable TransposeDT(DataTable dt){
    	DataTable dt_Transpose = new DataTable();
    	int columnSize = dt.getColumn(1).size();  //每个属性包含的属性值的个数  
    	int columnCount = dt.getColumnCount();    //属性个数 
    	for(int i=1; i<=columnSize; i++){
    		DataColumn dc = new DataColumn();
    		dc.setColumnName(Integer.toString(i));    //注意:这里的ColumnName已经设置为列号
    		for(int j=1; j<=columnCount; j++){
    			dc.add2ColumnList(dt.getColumn(j).getValue(i));
    		}
    		dt_Transpose.addColumn(dc);
    	}
    	return dt_Transpose;
    }
	
    
	// 在Console中输出Node
    public void PrintNode(Node node) { 
    	this.PrintListInteger(node.liGenCom);
    } 
    
	// 在Console中输出Nodes
    public void PrintNodes(List<Node> nodes) { 
    	for(int i = 0; i < nodes.size(); i++) {
    		this.PrintListInteger(nodes.get(i).liGenCom);
    	}
    	
    } 
    
	// 在Console中输出CiColumn
    public void PrintCiColumn(CiColumn ciC) { 
    	System.out.print(ciC.iD + " ");    
    	for (Map<String,String> map : ciC.columnList) { // 遍历datatrans(一条事务)
            for(Map.Entry<String,String> me : map.entrySet()){ 
            	System.out.print(me.getKey() + " "+ me.getValue() + " ");
           }
	}
    	System.out.print(ciC.parent1 + " ");   
    	System.out.print(ciC.parent2 + " ");  
    	System.out.print(ciC.flag);  
    }
    
	// 在Console中输出Ci
    public void PrintCiTable(CiTable ciT) { 
    	for(int i=1; i<=ciT.getCiColumncount(); i++){
    		PrintCiColumn(ciT.getCiColumn(i));
    		System.out.println("");
    	}
    } 

	// 在Console中输出EiColumn
	public void PrintEiColumn(EiColumn eiC) { 
	        	System.out.println(eiC.getEiStart() + " "+ eiC.getEiEnd());
	}


	// 在Console中输出Ei
	public void PrintEiTable(EiTable eiT) { 
		for(int i=1; i<=eiT.getEiColumncount(); i++){
			PrintEiColumn(eiT.getEiColumn(i));
		}
	}
	
	public void PrintFrequencySet(Map<List<String>, Integer> fs){
		for (Map.Entry<List<String>, Integer> me : fs.entrySet()) {
			List<String> ls = me.getKey();
			for(int i=0; i<ls.size(); i++){
				System.out.print(ls.get(i) + " ");
			}
			System.out.print(me.getValue());
			System.out.println("");
		}
	}
	
	public void PrintFsColumn(FsColumn fsc){
		
		
		// (1) 输出等价类属性集合
		this.PrintListString(fsc.Item);
		
		// (2) 输出该等价类对应的ID形成的List
//		this.PrintListInteger(fsc.list_ID);
		
		
		// (3) 输出等价类中元组个数
		System.out.println("该等价类中的元组的个数为：" + fsc.FsCount);
		
		// (4) 输出中心点的值
		double[][] centers = fsc.centers;
		int length = centers.length;
		int dim = centers[0].length;
		for(int i=0; i<length; i++){
			System.out.print("第" + i + "个中心点的坐标为：");
			for(int j=0; j<dim; j++){
				System.out.print(centers[i][j] + " ");
			}
			System.out.println("");
		}
		
		// (5) 输出sub_list_ID
		List<List<Integer>> sub_list_ID = fsc.sub_list_ID;
		for (int i = 0; i< sub_list_ID.size(); i++){
			System.out.print("第" + i + "个中心点对应的ID：");
			this.PrintListInteger(sub_list_ID.get(i));
		}
	}
	
	
	public void PrintFsTable(FsTable fst){
		int i = 1;
		for(FsColumn fsc:fst.lfs){
			System.out.println("");
			System.out.println("第" + i + "个频繁项集：");
			this.PrintFsColumn(fsc);
			i ++;
		}
	}
	
	
	public FsTable getFsTableSatisfyK(FsTable fst, int k) {
		FsTable result = new FsTable();
		for(int i = 0; i < fst.lfs.size(); i++) {
			if(fst.lfs.get(i).FsCount >= k) {
				result.add(fst.lfs.get(i));
			}
		}
		return result;
	}
	
	
	public DataTable delListIDInDataTable(DataTable dataTrans, FsTable fst_Satisfy_k) {
		for(int i = 0; i < fst_Satisfy_k.lfs.size(); i++) {
			dataTrans = this.delDTByID_List(dataTrans, fst_Satisfy_k.lfs.get(i).list_ID);
		}
		return dataTrans;
	}
	
	public DataTable delListIDInDataTable2(DataTable dataTrans, FsTable fst_Satisfy_k) {
		List<Integer> li = new ArrayList<Integer>();
		for(int i = 0; i < fst_Satisfy_k.lfs.size(); i++) {
			li.addAll(fst_Satisfy_k.lfs.get(i).list_ID);
		}
		dataTrans = this.delDTByID_List2(dataTrans, li);
		return dataTrans;
	}
	
	public double getILbyGenGroup(DataTable dt, List<Integer> lGenGroup, List<String> attQ) {
		
		// (1) 初始化帮助类
		DBHelper help=new DBHelper();
        MPPHelper mh = new MPPHelper();
        GenHelper gh = new GenHelper();  // 泛化帮助类
        
        // (2) 获得泛化后的数据表并转置
		DataTable dt_gen =  gh.getGenDataTable(dt, lGenGroup);          // 获得泛化数据
		dt_gen = mh.TransposeDT(dt_gen);           // 将泛化数据转置
		
		// (3) 获得频繁项集
		Map<List<String>, Integer> frequencySet = mh.getFrequencySet(dt_gen);   // 获得频繁项集
		
		// (4) 根据(2)和(3)的结构构建频率集表
		FsTable fst = mh.getFsTable(dt_gen, frequencySet);
		
		// (5) 构建IL_Table
		IL_Table ilt = new IL_Table();
		for(int w=1; w<=fst.lfs.size(); w++){
			// (5.1) 获得每一个频繁项集表中的Column
			FsColumn fsc = fst.getFsColumn(w);   // 到分类属性下的频繁项集
			
			// (5.2) 构建IL_Column并为其index进行赋值
			IL_Column ilc = new IL_Column();
			ilc.index = w;
			
			// (5.3) 对当前频繁项集Column中的每个属性都计算IL_Base并add进IL_Column
			for (int q = 0; q < attQ.size(); q++){

				IL_Base ilb = new IL_Base();    
				ilb.sAttrname = attQ.get(q);     // 对属性名按顺序进行赋值，保证一致性
				ilb.sClass = gh.gt.getGenColumn(ilb.sAttrname).attr_class;  // 对属性类别按顺序进行赋值，保证一致性
				ilb.iCount = fsc.FsCount;    // 对数量进行赋值
				if(ilb.sClass.equals("Numerical")) {
						String sSQL = "select " + ilb.sAttrname + " from adult";
				       	List<String> ls = help.GetListFromColumnbyNum(sSQL, fsc.list_ID);     // 注意lli.get(p)其实就是包含Num列表的频繁项集
			            Double max = Double.parseDouble(ls.get(0));     
			            Double min = Double.parseDouble(ls.get(0)); 
			            for (int r = 0; r < ls.size(); r++) {          
			                      if (min > Double.parseDouble(ls.get(r))) min = Double.parseDouble(ls.get(r));   
			                      if (max < Double.parseDouble(ls.get(r))) max = Double.parseDouble(ls.get(r));        
			            }
			            ilb.dMax = max;
			            ilb.dMin = min;
//						ilb.dMin = 0;
//						ilb.dMax = gh.gtd.getAttrRangeInGenLevel(ilb.sAttrname, node.getGenLevelByAttr(ilb.sAttrname));
			            ilb.dRange = gh.gt.getGenColumn(ilb.sAttrname).dRange;
					}
				else if(ilb.sClass.equals("Categorical")){
					ilb.dHSub = lGenGroup.get(q);
					ilb.dHAll = gh.gt.getGenColumn(ilb.sAttrname).maxGenLevel;
				}
				ilc.lil.add(ilb); 
			
			}
			 
			ilt.lcc.add(ilc);	
		}
		EvaluationMethod em = new EvaluationMethod();
		System.out.println("");
		System.out.println("IL = " + em.IL(ilt));
		
		return em.IL(ilt);

    }
    
    

	
	
	
	/**
	 * 计算频繁项集
	 * 
	 * @param preMap
	 *            保存频繁K项集的map
	 * @param tgFileWriter
	 *            输出文件句柄
	 * @return int 频繁i项集的数目
	 * @throws IOException
	 */
	public Map<List<String>, Integer> getFrequencySet(DataTable dt) {
		Map<List<String>, Integer> result = new HashMap<List<String>, Integer>();
		for(DataColumn dc:dt.dataT){
			if(result.containsKey(dc.columnList)){
				for (Map.Entry<List<String>, Integer> preMapItem : result.entrySet()) {
				    List<String> keyList = preMapItem.getKey();
					if(isListEqual(keyList,dc.columnList)){
						preMapItem.setValue(preMapItem.getValue()+1);
					}
				}
			}else{
				result.put(dc.columnList, 1);
			}
			
		}
		return result;		
	}


	// 获得满足k的频繁项集
	public Map<List<String>, Integer> getFrequencySetSatisfyK(Map<List<String>, Integer> frequencySet, int k) {
		Map<List<String>, Integer> result = new HashMap<List<String>, Integer>();
		for (Map.Entry<List<String>, Integer> me : frequencySet.entrySet()) {
			if(me.getValue() >= k) {
				result.put(me.getKey(), me.getValue());
			}
		}
		return result;
	}
	
	
	
	// 其中dt为泛化表,填充Item，FsCount和list_ID
	public FsTable getFsTable(DataTable gen_dt,Map<List<String>, Integer> ml) {
		FsTable fst = new FsTable();
		
			for (Map.Entry<List<String>, Integer> me : ml.entrySet()) {
				FsColumn fsc = new FsColumn();
				fsc.Item = me.getKey();
				fsc.FsCount = me.getValue();
				for(int i = 1; i <= gen_dt.dataT.size(); i++){
					if(isListEqual(gen_dt.getColumn(i).columnList,fsc.Item)){
						fsc.list_ID.add(i);
					}
				}
				fst.add(fsc);
			}
			
		return fst;
		
	}
	
	public FsTable getFsTable2(DataTable gen_dt,Map<List<String>, Integer> ml) {
		FsTable fst = new FsTable();
		int iMax = gen_dt.dataT.size();
		
			System.out.println("共" + ml.entrySet().size() + "个");
			int index = 1;
			for (Map.Entry<List<String>, Integer> me : ml.entrySet()) {
				System.out.println("当前第" + index + "个");
				index ++;
				
				FsColumn fsc = new FsColumn();
				fsc.Item = me.getKey();
				fsc.FsCount = me.getValue();
				// 下两行是用于优化的
				fsc.list_ID.ensureCapacity(me.getValue());
				
				
//				long startTime = System.currentTimeMillis(); // 设置开始时间
				for(int i = 1; i <= iMax; i++){
					if(isListEqual(gen_dt.getColumn(i).columnList,fsc.Item)){
						fsc.list_ID.add(i);
					}
				}
//				long endTime = System.currentTimeMillis(); // 设置结束时间
//				long totalTime = 0;
//				totalTime+= endTime - startTime; // 总时间
//				System.out.println("共用时：" + totalTime + "ms");
				fst.add(fsc);
			}
			
		return fst;
		
	}
	
	// 其中dt为泛化表,填充Item，FsCount和list_ID
	public FsTable getFsTableByDivideN(DataTable gen_dt, Map<List<String>, Integer> ml, int n, int k) {
		FsTable fst = new FsTable();
		
			for (Map.Entry<List<String>, Integer> me : ml.entrySet()) {
				// 第一步：获得频繁项集
				ArrayList<Integer> list_ID_All = new ArrayList<Integer>();
				for(int i = 1; i<=gen_dt.dataT.size(); i++){
					if(compare(gen_dt.getColumn(i).columnList, me.getKey())){
						list_ID_All.add(i);
					}
				}
				if (me.getValue() > n*k) {         // me.getValue()应该是和list_ID_All.size()一个数值
					int DivideN = (int) Math.floor(me.getValue()/(n*k));
					for (int index = 1; index <= DivideN; index ++) {
						if (index == DivideN) {
							FsColumn fsc = new FsColumn();
							fsc.Item = (ArrayList<String>) me.getKey(); 
							fsc.FsCount = list_ID_All.size() - index*n*k;
							fsc.list_ID = (ArrayList<Integer>) this.getListByDivideMN(list_ID_All, index*n*k,list_ID_All.size()-1);
							fst.add(fsc);
						}else{
							FsColumn fsc = new FsColumn();
							fsc.Item = (ArrayList<String>) me.getKey(); 
							fsc.FsCount = n*k;
							fsc.list_ID = (ArrayList<Integer>) this.getListByDivideMN(list_ID_All, (index-1)*n*k, index*n*k-1);
							fst.add(fsc);
						}
					}
				}else{
					FsColumn fsc = new FsColumn();
					fsc.Item = (ArrayList<String>) me.getKey();
					fsc.FsCount = me.getValue();
					fsc.list_ID = list_ID_All;
					fst.add(fsc);
				}
				
			}
			return fst;
		
	}
	
	
	public List<Integer> getListByDivideMN(List<Integer> ls, int M, int N) {
		List<Integer> result = new ArrayList<Integer>();
		for (int i = M; i <=N; i++) {
			result.add(ls.get(i));
		}
		return result;
	}
	
	
	
	public static <T extends Comparable<T>> boolean compare(List<T> a, List<T> b) {
	    if(a.size() != b.size())
	        return false;
//	    Collections.sort(a);
//	    Collections.sort(b);
	    for(int i=0;i<a.size();i++){
	        if(!a.get(i).equals(b.get(i)))
	            return false;
	    }
	    return true;
	}	
	
	public boolean isListEqual(List<String> ls1,List<String> ls2){
		boolean result = true;
		for(int i=0; i<ls1.size(); i++){
			if(!ls1.get(i).equals(ls2.get(i))){
//			if(ls1.get(i)!=ls2.get(i)){
				result = false;
				break;
			}
		}
		return result;
	}
	
	public boolean isk(Map<List<String>, Integer> fs,int k){
		boolean result = true;
		for (Map.Entry<List<String>, Integer> me : fs.entrySet()) {
			if(me.getValue() < k){
				result = false;
			}
		}
		return result;
	}
	
	
	
	public void markAllDirectGenNode(CiColumn node,CiTable Ci,EiTable Ei){
		// (1) 首先定义获得所有的泛化ID列表
		List<Integer> iDList = Ei.getAllDirectGenNode(node.iD);
		// (2) 标记自己
		System.out.print("MarkID" + node.iD + " ");
		// (2) 标记自己的父辈
		for(int i=0; i<iDList.size(); i++){
			Ci.markColumn(iDList.get(i));
		}
	}
	
	
	public double[][] convertDT2DoubleArray(DataTable dt){
		// 还缺少一个对String能否转换成double的判断
		
		int i = dt.dataT.size();
		int j = dt.dataT.get(0).columnList.size();
		
		double[][] result = new double[i][j];
		
		i = 0;
		for(DataColumn dc:dt.dataT){
			for(int m = 0; m< j; m++){
				result[i][m] = Double.valueOf(dc.columnList.get(m));
			}
			i++;
		}
		
		return result;
	}
	
	
	public int getNumFromID_ID_Dist(List<Integer> ID_Item, int ID1){
		for (int i = 0; i < ID_Item.size(); i++) {
			if(ID_Item.get(i) == ID1){
				return i;
			}
		}
		return 0;
	}
	
	
	
	// 根据由ID构成的List同步删除dt_for_delete
	public DataTable delDTByID_List(DataTable dt_for_delete, List<Integer> List_ID_of_firstK) {
		DataTable result = dt_for_delete;
		for (int i = 0; i < List_ID_of_firstK.size(); i++) {
			for (int j = 0; j < result.dataT.size(); j++){
				if (List_ID_of_firstK.get(i) == Integer.parseInt(result.dataT.get(j).columnName)){
					result.dataT.remove(j);
				}
			}
		}
		return result;
	}
	
	
	// 根据由ID构成的List同步删除dt_for_delete ----- 注意点，该方法删除和 delDTByID_List不同之处在于DataTable是横行的
	public DataTable delDTByID_List2(DataTable dataTrans, List<Integer> iForDel) {	
		DataTable dt_new = new DataTable();
		
	    int jMax = dataTrans.dataT.get(1).size();
	    
	    for(int i = 0; i < dataTrans.dataT.size(); i++) {
	    	List<String> lsc_new = new ArrayList<String>();
	    	for(int j = 0; j < jMax; j++) {
	    		int flag = 1;
	    		for(int m = 0; m < iForDel.size(); m ++) {
	    			Integer iForDelReal = iForDel.get(m)-1;
	    			if(j == iForDelReal){
	    				flag = 0;
	    				break;
	    			}
	    		}
	    		if(flag == 1) {
		    		String ss = dataTrans.dataT.get(i).columnList.get(j);
		    		lsc_new.add(ss);
	    		}
	    		
	    	}
	    	dt_new.addColumn(dataTrans.dataT.get(i).columnName,lsc_new);
	    }
	    return dt_new;
	}
	
	
	// 根据由ID构成的List同步删除ID_Item
	public List<Integer> delID_ItemByID_List(List<Integer> ID_Item, List<Integer> List_ID_of_firstK) {
		
		for (int i = 0; i < List_ID_of_firstK.size(); i++) {
			for (int j = 0; j < ID_Item.size(); j++){
				if(List_ID_of_firstK.get(i).equals(ID_Item.get(j))){
					ID_Item.remove(j);
				}
			}
		}
		return ID_Item;
	}
	
	
	// 方法：删除异常数据	
	/**delAbData
	 * 删除数据中带“？”的行
	 * 
	 * @dataTrans：事务数据集
	 * @abFlag：异常标志，可以为“？”、“ ”等
	 */
	public DataTable delAbData(DataTable dataTrans_pre,String abFlag) {
		DataTable dataTrans_new = new DataTable();
		Integer flag = 0;
		for(DataColumn dc:dataTrans_pre.dataT){
			flag = 0;
			for(String s:dc.columnList){
				if (s.equals(abFlag)) {
					flag = 1;
					break;
				} 
			}
			if (flag == 0) {
				dataTrans_new.addColumn(dc);;
			}
		}
		return dataTrans_new;
	}

	
	// 自动生成Ei
	public void AutoCreateEiTable(int n_attQ){
		DBHelper dh = new DBHelper();
		for(int i=1; i<=n_attQ; i++){
			String cSQL = "create table E" + i + "(EiStart int, EiEnd int)";
			dh.CreateDataBase(cSQL);
		}
	}
	
	// 自动删除Ei
	public void AutoDropEiTable(int n_attQ){
		DBHelper dh = new DBHelper();
		for(int i=1; i<=n_attQ; i++){
			String cSQL = "drop table E" + i;
			dh.CreateDataBase(cSQL);
		}
	}
	
	// 自动生成CandidateEdgesTable
	public void AutoCreateCandidateEdgesTable(){
		DBHelper dh = new DBHelper();
			String cSQL = "create table CandidateEdges(EiStart int,EiEnd int)";
			String cSQL1 = "create table CandidateEdges1(EiStart int,EiEnd int)";
			String cSQL2 = "create table CandidateEdges2(EiStart int,EiEnd int)";
			String cSQL3 = "create table CandidateEdges3(EiStart int,EiEnd int)";
			dh.CreateDataBase(cSQL);
			dh.CreateDataBase(cSQL1);
			dh.CreateDataBase(cSQL2);
			dh.CreateDataBase(cSQL3);
	}
	
	// 自动删除CandidateEdgesTable
	public void AutoDropCandidateEdgesTable(){
		DBHelper dh = new DBHelper();
			String cSQL = "drop table CandidateEdges";
			String cSQL1 = "drop table CandidateEdges1";
			String cSQL2 = "drop table CandidateEdges2";
			String cSQL3 = "drop table CandidateEdges3";
			dh.CreateDataBase(cSQL);
			dh.CreateDataBase(cSQL1);
			dh.CreateDataBase(cSQL2);
			dh.CreateDataBase(cSQL3);
	}


	
	// 自动生成C1
	public void AutoCreateC1Table(){
		DBHelper dh = new DBHelper();
		String cSQL = "create table C1(ID int IDENTITY(1,1) NOT NULL,dim1 nvarchar(50),index1 int)";
		dh.CreateDataBase(cSQL);
	}
	
	// 自动删除C1
	public void AutoDropC1Table(){
		DBHelper dh = new DBHelper();
		String cSQL = "drop table C1";
		dh.CreateDataBase(cSQL);
	}
	
	// 自动生成Ci（除C1）
	public void AutoCreateCiTable(int n_attQ){
		DBHelper dh = new DBHelper();
		for(int i=2; i<=n_attQ; i++){
			String cSQL = "create table C" + i + "(ID int IDENTITY(1,1) NOT NULL,";
			for(int j=1;j<=i;j++){
				cSQL = cSQL + "dim" + j + " nvarchar(50),index" + j + " int,";
			}
			cSQL = cSQL + "parent1 int,parent2 int)";
			dh.CreateDataBase(cSQL);
			String cSQL_ON = "SET IDENTITY_INSERT C" + i + " ON";
			dh.CreateDataBase(cSQL_ON);
		}
	}
	
	// 自动删除Ci（除C1）
	public void AutoDropCiTable(int n_attQ){
		DBHelper dh = new DBHelper();
		for(int i=2; i<=n_attQ; i++){
			String cSQL = "drop table C" + i;
			dh.CreateDataBase(cSQL);
		}
	}
	
	
	// 自动填充C1表
	public void AutoFillC1E1Table(List<String> attQ) throws SQLException{
		GenHelper gh = new GenHelper();
		DBHelper dh = new DBHelper();
		int iD = 1;
		for(int i=0; i<attQ.size(); i++){
			GenAttrLevel gal = gh.getAttrLevel(attQ.get(i));
			
			// 以下是显示部分，可以注释掉
//			System.out.print(gal.attr + " ");
//			for(int j=0; j<gal.level_Item.size(); j++){
//				System.out.print(gal.level_Item.get(j) + " ");
//			}
//			System.out.println("");
			
			
			// 写入C1和E1
			iD = dh.insert2C1E1(gal, iD);
			
		}
		
	}
	
	//去掉columnlist中的一个map
			public CiColumn removeAttr(int i,CiColumn ciC){
				CiColumn rNode = new CiColumn();
				for(int j =0;j<ciC.columnList.size();j++){
					if(i != j){
						rNode.columnList.add(ciC.columnList.get(j));
					}
				}
//				for(Map<String,String> col:ciC.columnList){
//					rNode.columnList.add(col);
//				}
				rNode.iD = ciC.iD;
				rNode.flag = ciC.flag;
				rNode.parent1 = ciC.parent1;
				rNode.parent2 = ciC.parent2;
				return rNode;
			}
	
}

