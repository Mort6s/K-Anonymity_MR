package OLA;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;





public class OLA {

	
	static List<Node> nodes = new ArrayList<Node>();
	static List<Node> minQueue = new ArrayList<Node>();
	static int recursionTimes = 0;    // ���ڱ�ʾ�ݹ����

	public static void main(String[] args) throws SQLException { 
		
		// ��¼��ʼʱ��
		long startTime = System.currentTimeMillis(); // ���ÿ�ʼʱ��
		
		// ��ʼ��������
		DBHelper help=new DBHelper();
        MPPHelper mh = new MPPHelper();
        GenHelper gh = new GenHelper();  // ����������
        OLAHelper oh = new OLAHelper();
        List<Map<String,Integer>> result = new ArrayList<Map<String,Integer>>();
        
		
		// (1) ���������ݽ��и�ֵ
		int k = public_data.k;   // k����
		List<String> attQ = public_data.attQ;
		
		// (2) ����DataTable
		DataTable dataTrans = new DataTable(); 
		int n_attQ = attQ.size(); 
		List<String> lsc = new ArrayList<String>();
        for(int i=0;i<n_attQ;i++){
        	String att_now = attQ.get(i).toString();
        	String sSQL = "select " + att_now + " from census_income";
	       	lsc = help.GetSingleColumn(sSQL);
	       	dataTrans.addColumn(att_now, lsc);
        }
		
		// (3) ����Generalization Lattice,��Nodes����ʽ�洢
        	// (3.1) ���ÿ�����Ե���󷺻����𲢴���liMaxAttrLevel��
			List<Integer> liMaxAttrLevel = new ArrayList<Integer>();
			int totalMaxAttrLevel = 0;
			for(int i = 0;i < n_attQ; i++) {
	        	String att_now = attQ.get(i).toString();
	        	int att_now_level = gh.getMaxAttrLevelbyAttrName(att_now);
	        	liMaxAttrLevel.add(att_now_level);
	        	totalMaxAttrLevel = totalMaxAttrLevel + att_now_level;
	        }
	        // (3.2) ���ȫ������Nodes
			for(int i1 = 0; i1 <= liMaxAttrLevel.get(0); i1++) {
				for(int i2 = 0; i2 <= liMaxAttrLevel.get(1); i2++) {
					for(int i3 = 0; i3 <= liMaxAttrLevel.get(2); i3++) {
//						for(int i4 = 0; i4 <= liMaxAttrLevel.get(3); i4++) {
//							for(int i5 = 0; i5 <= liMaxAttrLevel.get(4); i5++) {
//								for(int i6 = 0; i6 <= liMaxAttrLevel.get(5); i6++) {
//									for(int i7 = 0; i7 <= liMaxAttrLevel.get(6); i7++) {
										Node node = new Node();
										node.liGenCom.add(i1);
										node.liGenCom.add(i2);
										node.liGenCom.add(i3);
//										node.liGenCom.add(i4);
//										node.liGenCom.add(i5);
//										node.liGenCom.add(i6);
//										node.liGenCom.add(i7);
//										node.iGenLatLevel = i1 + i2 + i3 + i4 + i5 + i6 + i7;
										node.iGenLatLevel = i1 + i2 + i3;
										nodes.add(node);
//									}
//								}
//							}
//						}
					}
				}
			}
			// (3.3) �鿴ȫ������Nodes
//			for(int i = 0; i < nodes.size(); i++) {
//				mh.PrintNode(nodes.get(i));
//			}
			
		// (4) ���middleQueue
		List<Node> middleQueue = new ArrayList<Node>();
		middleQueue = oh.getMiddleQueue(0, totalMaxAttrLevel);
		
//		mh.PrintNodes(middleQueue);
//		System.out.println("");
		
		// (5) �ؼ����㲽��
	    oh.getMinQueueForNodes(dataTrans, middleQueue, k, 0, totalMaxAttrLevel);
	    
	    // (6) ������
	    System.out.println("�������minQueueΪ��");
		mh.PrintNodes(OLA.minQueue);
		
	    // (6) ��һ��ɾѡ
	    List<Node> lastResult = oh.getFiltrate(OLA.minQueue);
		
	     
	    for(int i=0; i< lastResult.size(); i++){
	    	Map<String,Integer> temp = new HashMap<String,Integer>();
	        for(int j=0; j< attQ.size(); j++){
	        temp.put(attQ.get(j),lastResult.get(i).liGenCom.get(j));
	        }
	    	result.add(temp);;
	    }
	    
	    
	 // �����ս�����뵽SQL��Ci��
    	help.insertResult(result,3);    // �������ݿ��е�Ci��
	    
	    // (7) ������ս��
	    System.out.println("����ɸѡ������ս��Ϊ��");
		mh.PrintNodes(lastResult);
		
		

    	
        	
        	
        // ����㷨ʱ��
		long endTime = System.currentTimeMillis(); // ���ý���ʱ��
		long totalTime = endTime - startTime; // ��ʱ��
		System.out.println("OLA�㷨����ʱ��" + totalTime + "ms");
	}

	
}
