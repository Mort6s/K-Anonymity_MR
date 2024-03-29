package Incognito_SQLExpress_Census;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBHelper {
	Connection _CONN = null;
	
	private boolean GetConn() {
		if(_CONN!=null)return true;
		try {
			String address = "jdbc:sqlserver://192.168.100.1:1433;";
			String user = "sa";
			String pwd = "asdf1234*";
			String database = "adult";
			
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
			_CONN = DriverManager.getConnection(address, user, pwd);
			_CONN.setCatalog(database);

		} catch (Exception ex) {
			// ex.printStackTrace();
			System.out.println(ex.getMessage());
			return false;
		}
		return true;
	}
		
	private void CloseConn()
	{
		try {
			_CONN.close();
			_CONN = null;
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
			_CONN=null;	
		}
	}
 
	public boolean TestConn() {
		if (!GetConn())
			return false;

		CloseConn();
		return true;
	}
	
	public ResultSet GetResultSet(String sSQL,Object[] objParams)
	{
		GetConn();
		ResultSet rs=null;
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if(objParams!=null)
			{
				for(int i=0;i< objParams.length;i++)
				{
					ps.setObject(i+1, objParams[i]);
				}
			}
			rs=ps.executeQuery();
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			CloseConn();
		}
		finally
		{
			//CloseConn();			
		}
		return rs;
	}
	
	public Object GetSingle(String sSQL,Object... objParams)
	{
		GetConn();
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if(objParams!=null)
			{
				for(int i=0;i< objParams.length;i++)
				{
					ps.setObject(i+1, objParams[i]);
				}
			}
			ResultSet rs=ps.executeQuery();
			if(rs.next())
				return rs.getString(1);
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
		finally
		{
			CloseConn();			
		}
		return null;
	}
	
	public List<String> GetSingleColumn(String sSQL)
	{
		GetConn();
		List<String> ls = new ArrayList<String>();
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			ResultSet rs=ps.executeQuery();
			while(rs.next()){
				ls.add(rs.getString(1).trim());
			}
			return ls;
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
		finally
		{
			CloseConn();			
		}
		return null;
	}	
	
	public List<String> GetListFromColumnbyNum(String sSQL, List<Integer> list_Num)
	{
		GetConn();
		List<String> ls = new ArrayList<String>();
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			ResultSet rs=ps.executeQuery();
			int index = 1;
			while(rs.next()){
				if(list_Num.contains(index)){
					ls.add(rs.getString(1).trim());
				}
				index ++;
			}
			return ls;
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
		finally
		{
			CloseConn();			
		}
		return null;
	}
	
	public int UpdateData(String sSQL,Object[] objParams)
	{
		GetConn();
		int iResult=0;
		
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if(objParams!=null)
			{
				for(int i=0;i< objParams.length;i++)
				{
					ps.setObject(i+1, objParams[i]);
				}
			}
			iResult = ps.executeUpdate(sSQL);
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			return -1;
		}
		finally
		{
			CloseConn();			
		}
		return iResult;
	}
	
	public int CreateDataBase(String sSQL)
	{
		GetConn();
		int iResult=0;
		
		try
		{
			Statement ps = _CONN.createStatement();
			iResult = ps.executeUpdate(sSQL);
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			return -1;
		}
		finally
		{
			CloseConn();			
		}
		return iResult;
	}
	
	public int CreateDataTable(String sSQL)
	{
		GetConn();
		int iResult=0;
		
		try
		{
			Statement ps = _CONN.createStatement();
			iResult = ps.executeUpdate(sSQL);
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			return -1;
		}
		finally
		{
			CloseConn();			
		}
		return iResult;
	}
	
    public int insertDatabySQL(String sSQL,Object[] objParams) throws SQLException{
         
    	GetConn();
		int iResult=0;
		
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if(objParams!=null)
			{
				for(int i=0;i< objParams.length;i++)
				{
					ps.setObject(i+1, objParams[i]);
				}
			}
			iResult = ps.executeUpdate();

		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			return -1;
		}
		finally
		{
			CloseConn();			
		}
		return iResult;

    }
    
    
    public void insert2SQLFromDataTable(String tableName,DataTable dt) throws SQLException{	
    	GetConn();
    	Statement stmt = _CONN.createStatement();
    	
        try{
        	int row = dt.getColumnCount();
        	int col = dt.getColumn(1).size();
        	
        	for(int i=1;i<=row;i++){
            	String sSQL="insert into " + tableName + " values('" + dt.getColumn(i).columnList.get(0) + "'";
            	
        		for(int j=2;j<=col;j++){       			
        			sSQL = sSQL + ",'" + dt.getColumn(i).columnList.get(j-1) + "'";
        		}
            	
            	sSQL = sSQL + ")";
            	stmt.addBatch(sSQL);
        	}
        	stmt.executeBatch();
        }catch(Exception ex) 
        { 
            System.out.println("ERROR: " + ex.getMessage()); 
        }finally
		{
			CloseConn();		
		}
    }
    
    public int[] test() throws SQLException{
    	
    	GetConn();
    	Statement stmt_c1 = _CONN.createStatement();
    	String sSQL = "set identity_insert C1 on";
    	stmt_c1.addBatch(sSQL);
    	sSQL="insert into C1(ID,dim1,index1) values(18,'abc',6)";
    	stmt_c1.addBatch(sSQL);
    	int[] result = stmt_c1.executeBatch();
    	
    	return result;
    }
    
    
    @SuppressWarnings("finally")
	public int insert2C1E1(GenAttrLevel gal,int iD) throws SQLException{	
    	GetConn();
    	Statement stmt_c1 = _CONN.createStatement();
    	Statement stmt_e1 = _CONN.createStatement();
    	
    	String sSQL_Ide = "set identity_insert C1 on";
    	stmt_c1.addBatch(sSQL_Ide);
    	
        try{
        	int row = gal.level_Item.size();
        	for(int i=0;i<row;i++){
        		
        		String sSQL_C1="insert into C1(ID,dim1,index1) values(" + iD + ",'" + gal.attr + "'," + gal.level_Item.get(i) + ")";
            	stmt_c1.addBatch(sSQL_C1);
            	if(i<row-1){
                	int E1Start = iD;
                	int E1End = iD +1;
                	String sSQL_E1="insert into E1 values(" + E1Start + "," + E1End + ")";
                	stmt_e1.addBatch(sSQL_E1);
            	}
            	iD++;
        	}
        	stmt_c1.executeBatch();
        	stmt_e1.executeBatch();
        	return iD;
        }catch(Exception ex) 
        { 
            System.out.println("ERROR: " + ex.getMessage()); 
            return iD;
        }finally{
			CloseConn();	
			return iD;
		}
    }
    
    public void insert2Ci(CiTable ciT, int i) throws SQLException{
    	GetConn();
    	Statement stmt_ci = _CONN.createStatement();
        try{
        	String sSQL_Ci_Trun = "truncate table C" + i;
        	stmt_ci.addBatch(sSQL_Ci_Trun);
        	String sSQL_Ide = "set identity_insert C" + i +" on";
        	stmt_ci.addBatch(sSQL_Ide);
        	for(CiColumn cic:ciT.lCi){
        		String sSQL_1="insert into C" + i + "("; 
        		
        		String sSQL_2 = "ID";
        		for(int k=1;k<=i;k++){
        			sSQL_2 = sSQL_2 + ",dim" + k + ",index" + k;
        		}
        		String sSQL_3 = " values(" + cic.iD;
        		for(Map<String,String> ms:cic.columnList){
        			for (Map.Entry<String,String> me : ms.entrySet()){
        				sSQL_3 = sSQL_3 + ",'" + me.getKey() + "'," + Integer.parseInt(me.getValue());
        			}
        		}

        		if(i!=1){
        			sSQL_2 = sSQL_2 + ",parent1,parent2";
        			sSQL_3 = sSQL_3 + "," + cic.parent1 + "," + cic.parent2;
        		}
        		String sSQL_Ci = sSQL_1 + sSQL_2 + ")" + sSQL_3 + ")";
        		stmt_ci.addBatch(sSQL_Ci);
        	}
    		stmt_ci.executeBatch();
        }catch(Exception ex) 
        { 
            System.out.println("ERROR: " + ex.getMessage()); 
        }finally{
			CloseConn();	
		}
    }
    
    public void insert2NextCi(int i) throws SQLException{
    	GetConn();
    	Statement stmt_ci = _CONN.createStatement();
    	
    	String sSQL_1 = "C" + i + " (dim1,index1";
    	for(int k=2;k<=i;k++){
    		sSQL_1 = sSQL_1 + ",dim" + k + ",index" + k;
    	}
    	sSQL_1 = sSQL_1 + ",parent1,parent2)";
    	
    	int j = i-1;
    	
    	String sSQL_2 = "p.dim1,p.index1";
    	for(int k=2;k<=i-1;k++){
    		sSQL_2 = sSQL_2 + ",p.dim" + k +",p.index" + k;
    	}
    	sSQL_2 = sSQL_2 + ",q.dim" + j + ",q.index" + j;
    	sSQL_2 = sSQL_2 + ",p.ID,q.ID";
    	
    	
    	String sSQL_3 = "C" + j + " p,C" + j + " q";
    	
    	String sSQL_4 = "";
    	if(i>2){
    		sSQL_4 = "p.dim1 = q.dim1 and p.index1 = q.index1 and";
    	}
    	for(int k=2;k<=i-2;k++){
    		sSQL_4 = sSQL_4 + " p.dim" + k + " = q.dim" + k + " and p.index" + k + " = q.index" + k + " and";
    	}
    	
    	sSQL_4 = sSQL_4 + " p.dim" + j + " < q.dim" + j;
    	
    	String sSQL_Ci = "insert into " + sSQL_1 + " SELECT " + sSQL_2 + " FROM " + sSQL_3 + " WHERE " + sSQL_4;
    	
        try{
        	 stmt_ci.addBatch(sSQL_Ci);
        	 stmt_ci.executeBatch();
        }catch(Exception ex) 
        { 
            System.out.println("ERROR: " + ex.getMessage()); 
        }finally{
			CloseConn();	
		}
    	
    }
    
    
    public void insert2NextEi(int i) throws SQLException{
    	GetConn();
    	Statement stmt_ce = _CONN.createStatement();
    	Statement stmt_ce1 = _CONN.createStatement();
    	Statement stmt_ce2= _CONN.createStatement();
    	Statement stmt_ce3= _CONN.createStatement();
    	Statement stmt_ei = _CONN.createStatement();
    	
    	String sSQL_CE_Trun = "truncate table CandidateEdges";
    	String sSQL_CE_Trun1 = "truncate table CandidateEdges1";
    	String sSQL_CE_Trun2 = "truncate table CandidateEdges2";
    	String sSQL_CE_Trun3 = "truncate table CandidateEdges3";
    	stmt_ce.addBatch(sSQL_CE_Trun);
    	stmt_ce.addBatch(sSQL_CE_Trun1);
    	stmt_ce.addBatch(sSQL_CE_Trun2); 
    	stmt_ce.addBatch(sSQL_CE_Trun3); 
    	stmt_ce.executeBatch(); 
    	int j = i-1;
    	String sSQL_11="CandidateEdges1(Eistart,EiEnd) ";
    	String sSQL_12 = "p.ID as PID, q.ID as QID";
    	String sSQL_13 = "C" + i + " p, C" + i + " q, E" + j + " e,E" + j + " f";
    	String sSQL_14 = "(e.EiStart = p.parent1 and e.EiEnd = q.parent1 and f.EiStart = p.parent2 and f.EiEnd = q.parent2)";
    	String sSQL_ce1 = "insert into " + sSQL_11 + " SELECT " + sSQL_12 + " FROM " + sSQL_13 + " WHERE " + sSQL_14;
    	stmt_ce1.addBatch(sSQL_ce1);
    	stmt_ce1.executeBatch(); 
    	String sSQL_21="CandidateEdges2(Eistart,EiEnd)";
    	String sSQL_22 = "p.ID as PID, q.ID as QID ";
    	String sSQL_23 = "C" + i + " p, C" + i + " q, E" + j + " e,E" + j + " f";
    	String sSQL_24 = "(e.EiStart = p.parent1 and e.EiEnd = q.parent1 and p.parent2 = q.parent2)";
    	String sSQL_ce2 = "insert into " + sSQL_21 + " SELECT " + sSQL_22 + " FROM " + sSQL_23 + " WHERE " + sSQL_24;
    	stmt_ce2.addBatch(sSQL_ce2);
    	stmt_ce2.executeBatch(); 
    	String sSQL_31="CandidateEdges3(Eistart,EiEnd) ";
    	String sSQL_32 = "p.ID as PID, q.ID as QID";
    	String sSQL_33 = "C" + i + " p, C" + i + " q, E" + j + " e,E" + j + " f";
    	String sSQL_34 = "(e.EiStart = p.parent2 and e.EiEnd = q.parent2 and p.parent1 = q.parent1)";
    	String sSQL_ce3 = "insert into " + sSQL_31 + " SELECT " + sSQL_32 + " FROM " + sSQL_33 + " WHERE " + sSQL_34;
    	stmt_ce3.addBatch(sSQL_ce3);
    	String sSQL_1="insert into CandidateEdges select * from CandidateEdges1";
    	stmt_ce3.addBatch(sSQL_1);
    	String sSQL_2="insert into CandidateEdges select * from CandidateEdges2";
    	stmt_ce3.addBatch(sSQL_2);
    	String sSQL_3="insert into CandidateEdges select * from CandidateEdges3";
    	stmt_ce3.addBatch(sSQL_3);
    	stmt_ce3.executeBatch(); 
    	String sSQL_ei = "insert into E" + i + " (Eistart,EiEnd) SELECT D.EiStart, D.EiEnd FROM CandidateEdges D EXCEPT SELECT D1.EiStart, D2.EiEnd FROM CandidateEdges D1, CandidateEdges D2 WHERE D1.EiEnd = D2.EiStart";
    	stmt_ei.addBatch(sSQL_ei);
    	try{
    		 
        	 stmt_ei.executeBatch();
        }catch(Exception ex) 
        { 
            System.out.println("ERROR: " + ex.getMessage()); 
        }finally{
			CloseConn();	
		}
    }

    public void insert2NextEi2(int i) throws SQLException{
    	
    	String Ci = "C" + Integer.toString(i);
    	CiTable ciT = this.readSql2Ci(Ci);
    	
    	EiTable eiT = new EiTable();
    	
    	for(int p = 0; p < ciT.lCi.size(); p++) {

    		List<Map<String,String>> p_columnList = ciT.lCi.get(p).columnList;
    		
    		for(int q = 0; q < ciT.lCi.size(); q++) {
    			
    			List<Map<String,String>> q_columnList = ciT.lCi.get(q).columnList;
    			
    			int p_totalGenLevel = 0;
    			int q_totalGenLevel = 0;
    			
    			int flag = 1;
    			for(int z = 0; z < p_columnList.size(); z++) {
    				Map<String,String> pMap = p_columnList.get(z);
    				String pKey = "";
    				int pValue = 0;
    				for (Map.Entry<String,String> pME : pMap.entrySet()) { 
    					pKey = pME.getKey();
    					pValue = Integer.parseInt(pME.getValue());
    				}
    				Map<String,String> qMap = q_columnList.get(z);
    				String qKey = "";
    				int qValue = 0;
    				for (Map.Entry<String,String> qME : qMap.entrySet()) { 
    					qKey = qME.getKey();
    					qValue = Integer.parseInt(qME.getValue());
    				}    				
    				if(pKey.equals(qKey) && pValue <= qValue) {
    					p_totalGenLevel = p_totalGenLevel + pValue;
    					q_totalGenLevel = q_totalGenLevel + qValue;
    				}else{
    					flag = 0;
    					break;
    				}
    			}
    			if(flag == 1 && q_totalGenLevel-p_totalGenLevel==1) {
    				EiColumn eiC = new EiColumn();
    				eiC.EiStart = ciT.lCi.get(p).iD;
    				eiC.EiEnd = ciT.lCi.get(q).iD;
    				eiT.lEi.add(eiC);
    			}
    		}
    	}
     	GetConn();
    	Statement stmt = _CONN.createStatement();
        try{
        	for(int f = 0; f < eiT.lEi.size(); f++) {
            	String sSQL="insert into E" + i + " values('" + eiT.lEi.get(f).EiStart + "','" + eiT.lEi.get(f).EiEnd + "')";
            	stmt.addBatch(sSQL);
        	}
        	stmt.executeBatch();
        }catch(Exception ex) 
        { 
            System.out.println("ERROR: " + ex.getMessage()); 
        }finally
		{
			CloseConn();		
		}    	
    	
    }
    
    public void insert2E1(GenAttrLevel gal,GenTable gt) throws SQLException{	
    	GetConn();
    	Statement stmt = _CONN.createStatement();
    	
        try{
        	int row = gal.level_Item.size();
        	
        	for(int i=0;i<row-1;i++){
            	String sSQL="insert into E1 values(" + gal.level_Item.get(i) + "," + gal.level_Item.get(i+1) + ")";
            	stmt.addBatch(sSQL);
        	}
        	stmt.executeBatch();
        }catch(Exception ex) 
        { 
            System.out.println("ERROR: " + ex.getMessage()); 
        }finally
		{
			CloseConn();		
		}
    }

	public List<Map<String,String>> readTrans2LLM(String sSQL,List<String> attl,Object[] objParams) {
		
		GetConn();
		ResultSet rs=null;
		List<Map<String,String>> lmap = new ArrayList<Map<String,String>>();
		
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			if(objParams!=null)
			{
				for(int i=0;i< objParams.length;i++)
				{
					ps.setObject(i+1, objParams[i]);
				}
			}
			rs=ps.executeQuery();
			
			
			while(rs.next()){ 
				 Map<String, String> map = new HashMap<String, String>();
	             for(int i=0;i<attl.size();i++){ 
	            	 map.put(attl.get(i).toString(), rs.getString(attl.get(i).toString()));  
	            	 
	             }
	             lmap.add(map);
	         }
			
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			CloseConn();
		}
		finally
		{
			CloseConn();			
		}
		return lmap;
	}
    
	public void writeLLM2SqlServer(List<Map<String,String>> lmap,List<String> attl,String sTableName) {
		String flag = "?";
		for(int i=1;i<attl.size();i++)
		{
			flag = flag + ",?";
		}
		String sSQL="insert into " + sTableName + "values(" + flag + ");";
		GetConn();
		try{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			for (Map<String,String> map : lmap) {
		              for(int j=0;j<attl.size();j++){ 
		            	 ps.setObject(j+1, map.get(attl.get(j).toString()));
		             }
				ps.addBatch();
			}
			ps.executeBatch();
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			CloseConn();
		}
		finally
		{
			CloseConn();			
		}		
	}
	
	public CiTable readRootNode2Ci(String sTable, String eTable){
		GetConn();
		ResultSet rs=null;
		CiTable ciT = new CiTable();
		String sSQL = "Select * From " + sTable + " Where ID not in (Select distinct EiEnd From " + eTable + " where EiStart in (Select ID From " + sTable + "))";
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			rs=ps.executeQuery();
			while(rs.next()){ 
				CiColumn ciC = new CiColumn();
				ciC.setiD(Integer.parseInt(rs.getString(1)));
				ResultSetMetaData rsmd = rs.getMetaData();    
				int columnCount = rsmd.getColumnCount(); 
				if(columnCount < 5){	
					Map<String,String> map = new HashMap<String,String>();
					map.put(rs.getString(2), rs.getString(3));
					ciC.addMap2columnList(map);
				}else{
					for(int i=2;i<=columnCount-2;i++){
						if(i%2 == 0){
							Map<String,String> map = new HashMap<String,String>();
							map.put(rs.getString(i), rs.getString(i+1));
							ciC.addMap2columnList(map);
							
						}
					}
					ciC.setparent1(Integer.parseInt(rs.getString(columnCount-1)));
					ciC.setparent2(Integer.parseInt(rs.getString(columnCount)));
				}

				ciT.addCiColumn(ciC);
	        
	         }
			return ciT;
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			CloseConn();
		}
		finally
		{
			CloseConn();			
		}
		return ciT;
	}
	
	public CiTable readSql2Ci(String sTable) {
		GetConn();
		ResultSet rs=null;
		CiTable ciT = new CiTable();
		String sSQL = "Select * from " + sTable;
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			rs=ps.executeQuery();
			while(rs.next()){ 
				CiColumn ciC = new CiColumn();
				ciC.setiD(Integer.parseInt(rs.getString(1)));
				ResultSetMetaData rsmd = rs.getMetaData();    
				int columnCount = rsmd.getColumnCount();
				if(columnCount < 5){		
					Map<String,String> map = new HashMap<String,String>();
					map.put(rs.getString(2), rs.getString(3));
					ciC.addMap2columnList(map);
				}else{
					for(int i=2;i<=columnCount-2;i++){
						if(i%2 == 0){
							Map<String,String> map = new HashMap<String,String>();
							map.put(rs.getString(i), rs.getString(i+1));
							ciC.addMap2columnList(map);							
						}
					}
					ciC.setparent1(Integer.parseInt(rs.getString(columnCount-1)));
					ciC.setparent2(Integer.parseInt(rs.getString(columnCount)));
				}

				ciT.addCiColumn(ciC);
	        
	         }
			return ciT;
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			CloseConn();
		}
		finally
		{
			CloseConn();			
		}
		return ciT;
	}
	
	public EiTable readSql2Ei(String sTable) {
		GetConn();
		ResultSet rs=null;
		EiTable eiT = new EiTable();
		String sSQL = "Select * from " + sTable;
		try
		{
			PreparedStatement ps = _CONN.prepareStatement(sSQL);
			rs=ps.executeQuery();
			while(rs.next()){ 
				EiColumn eiC = new EiColumn();
				eiC.setEiStart(Integer.parseInt(rs.getString(1)));
				eiC.setEiEnd(Integer.parseInt(rs.getString(2)));
				eiT.addEiColumn(eiC);
				}
			return eiT;	
		}catch(Exception ex)
		{
			System.out.println(ex.getMessage());
			CloseConn();
		}
		finally
		{
			CloseConn();			
		}
		return eiT;
	}
}

