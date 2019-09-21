package Incognito_SQLExpress_Census;

import java.sql.SQLException;
import java.util.List;

public class Main_Preparation_Work {

	public Main_Preparation_Work(){};

	public void initTable() throws SQLException {
		
		List<String> attQ = public_data.attQ;

		MPPHelper mh = new MPPHelper();

		mh.AutoDropC1Table();
		mh.AutoDropCiTable(attQ.size());
		mh.AutoDropEiTable(attQ.size());
		mh.AutoDropCandidateEdgesTable();
        
		mh.AutoCreateC1Table();
		mh.AutoCreateCiTable(attQ.size());
		mh.AutoCreateEiTable(attQ.size());
		mh.AutoFillC1E1Table(attQ);
		mh.AutoCreateCandidateEdgesTable();
	}
}
