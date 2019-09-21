package OLA;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
 
 


public class OLAHelper {

    GenTableDefinition_census_income gtd = new GenTableDefinition_census_income();   // 定义泛化表类
    GenTable gt = gtd.getGenTable();   // 调用类中的方法获得泛化表
    MPPHelper mh = new MPPHelper();    // 帮助类
    GenHelper gh = new GenHelper();  // 泛化帮助类
    
    
    
    // 返回tag为0的Nodes
	public List<Node> getNodesByGenLatLevel(int iGenLatLevel) {
		List<Node> result = new ArrayList<Node>();
		for(int i = 0; i < OLA.nodes.size(); i++) {
			if(OLA.nodes.get(i).iGenLatLevel == iGenLatLevel && OLA.nodes.get(i).tag == 0) {
				result.add(OLA.nodes.get(i));
			}
		}
		return result;
	}
	
	
	// 方法：根据iLow和iHigh获得中间泛化级别对应的nodes
	public List<Node> getMiddleQueue(int iLow, int iHigh) {
		// (1) 根据iLow和iHigh获得中间的泛化格级别
		int iGenLatLevel = (int) Math.floor((iHigh - iLow)/2) + iLow;
		// (2) 取得middleQueue
		List<Node> middleQueue = new ArrayList<Node>();
		middleQueue = getNodesByGenLatLevel(iGenLatLevel);
		return middleQueue;
	}
	
	
//	public List<Node> getMinQueue(List<Node> nodes, int iLow, int iHigh) {
//		// (1) 首先判断node不为空
//		if(nodes != null) {
//			// () 对nodes进行循环
//			for(int i = 0; i < nodes.size(); i ++) {
//				node_now = nodes.get(i);
//				List<Node> middleQueue = getMiddleQueue(int iLow, int iHigh);
//						
//			}
//		}	
//	}
	
//	public List<Node> getMinQueueForSingleNode(DataTable dataTrans, Node node, int k, int iLow, int iHigh) {
//		
//	}
	
	
	// OLA最核心的函数
	public void getMinQueueForNodes(DataTable dataTrans, List<Node> nodes, int k, int iLow, int iHigh) {
		
		// (1) 当nodes节点的数量大于0的时候，会一直循环
		while(nodes.size() > 0) {
			// (2) 获得第一个node做后续处理，同时将其在nodes中删除
			Node node = nodes.get(0);
			nodes.remove(0);
			
			// (3) 判断该node的tag是否为0，如果标记为1或者2，那么意味着不需要计算了
			if(node.tag == 0) {
				// (0) 这里是做统计的
				OLA.recursionTimes = OLA.recursionTimes + 1;
				System.out.println("当前是第" + OLA.recursionTimes + "次计算频繁项集的过程！");
				
				// (4) 判断当前节点是否满足k匿名
					// (4.1) 获得泛化表并转置(便于计算)
					DataTable dt_gen = getGenDataTable(dataTrans, node.liGenCom);   
					dt_gen = mh.TransposeDT(dt_gen);           // 将泛化数据转置
					
					// (4。2) 获得频繁项集
					Map<List<String>, Integer> frequencySet = new HashMap<List<String>, Integer>();
					frequencySet =  mh.getFrequencySet(dt_gen);
					
					// (4.3) 通过频繁项集可以判断是否满足k
					if(mh.isk(frequencySet, k)){
						OLA.minQueue.add(node);   // 如果满足k匿名，先装进minQueue
						markAllGenNode(node);    // 向上mark为OK，然后向下取1/2
						if(node.iGenLatLevel - iLow == 1) {
							// 什么都不做
						} else {
							List<Node> nodes_middle = getMiddleQueue(iLow, node.iGenLatLevel);  // 获得中间队列的时候有tag的判断，并不是所有的都会加进来	 
							// () 非常关键的一部，递归
							getMinQueueForNodes(dataTrans, nodes_middle, k, iLow, node.iGenLatLevel);
						}
					}else{
						markAllFatherNode(node);   // 向下mark为del，然后向上取1/2
						if(iHigh - node.iGenLatLevel== 1) {
							// 什么都不做
						} else {
							List<Node> nodes_middle = getMiddleQueue(node.iGenLatLevel,iHigh);  // 获得中间队列的时候有tag的判断，并不是所有的都会加进来	 
							// () 非常关键的一部，递归
							getMinQueueForNodes(dataTrans, nodes_middle, k, node.iGenLatLevel ,iHigh);
						}
					}
			}
			
		}
		
	}
	
	
	// 筛选结果
	public List<Node> getFiltrate(List<Node> liNode) {
		List<Node> result = new ArrayList<Node>();
		for(int i = 0; i < liNode.size(); i ++) {
			Node node = liNode.get(i);
			int flag = 1;
			for(int j = 0; j < liNode.size(); j ++) {
				// 判断当前节点是不是所有节点中某个节点的泛化节点，如果是的，那么就把flag设置为0
				if(isGenNode(node.liGenCom,liNode.get(j).liGenCom)) {
					flag = 0;
				}
			}
			// 如果经过遍历后，该节点不是任何节点的父节点，那么可以加入到最终结果中
			if(flag == 1) {
				result.add(node);
			}
		}
		return result;
	}
	
	// 判断li1是否为li2的泛化节点(要求每一个都要大于等于后者，且总数要大于后者)
	public boolean isGenNode(List<Integer> li1, List<Integer> li2) {
		boolean result = false;
		int li1sum = 0;
		int li2sum = 0;
		int flag = 1;
		for(int i = 0; i < li1.size(); i ++) {
			li1sum = li1sum + li1.get(i);
			li2sum = li2sum + li2.get(i);
			if(li1.get(i) < li2.get(i)) {
				flag = 0;
			}
		}
		if(flag == 1) {
			if (li1sum > li2sum) {
				result = true;
			}
		}
		return result;
	}
	
	
	// 方法：获得node的所有泛化节点
	public void markAllGenNode(Node node) {
		  
		 int iMax = OLA.nodes.size();
		 int jMax = OLA.nodes.get(0).liGenCom.size();
		 for(int i = 0; i < iMax; i++) { 
			 Node node_now = OLA.nodes.get(i);
			 List<Integer> liGenCom = node_now.liGenCom;
			 int flag = 1;
			 for(int j = 0; j < jMax; j++) {
				 if(liGenCom.get(j) < node.liGenCom.get(j)) {
					 flag = 0;
					 break;
				 }
			 }
			 if(flag == 1) {
				 OLA.nodes.get(i).tag = 1;
//				 System.out.println("mark的节点为：" + OLA.nodes.get(i).iGenLatLevel);
			 }
		 }
		  
	}
	
	// 方法：获得node的所有父节点
	public void markAllFatherNode(Node node) {
		  
		 int iMax = OLA.nodes.size();
		 int jMax = OLA.nodes.get(0).liGenCom.size();
		 for(int i = 0; i < iMax; i++) { 
			 Node node_now = OLA.nodes.get(i);
			 List<Integer> liGenCom = node_now.liGenCom;
			 int flag = 1;
			 for(int j = 0; j < jMax; j++) {
				 if(liGenCom.get(j) > node.liGenCom.get(j)) {
					 flag = 0;
					 break;
				 }
			 }
			 if(flag == 1) {
				 OLA.nodes.get(i).tag = 2;
			 }
		 }
		 
	}
	
	
	// 根据属性获得泛化属性，其中liGenCom表示泛化组合
	public DataTable getGenDataTable(DataTable dt, List<Integer> liGenCom){
		DataTable dt_gen = new DataTable();
		for(int i = 0; i < liGenCom.size(); i++) {
			DataColumn dc_pregen = dt.getColumn(i+1);
			DataColumn dc_gen = gh.getGenDataColumn(dc_pregen,liGenCom.get(i));
			dt_gen.addColumn(dc_gen);
		}
		return dt_gen;
	}
	
	
	 
}
