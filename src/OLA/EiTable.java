package OLA;

import java.util.ArrayList;
import java.util.List;

public class EiTable {

List<EiColumn> lEi = new ArrayList<EiColumn>();
	
	public EiTable() {};
	
	
	// 添加节点/列
	public void addEiColumn(EiColumn eic) {
		lEi.add(eic);
	}
	
	// 根据num获得CiColumn
	public EiColumn getEiColumn(int num) {
		int i = 0;
		for(EiColumn eiC: lEi){
			i ++;
			if(i == num){
				return eiC;
			}
		}
		return null;
	}
	
	// 获得行数
	public int getEiColumncount(){
		return lEi.size();
	}
	
	// 获得直接泛化的点
	public List<Integer> getDirectGenNode(List<Integer> startList){
		List<Integer> result = new ArrayList<Integer>();
//		try{
			for(int i=0; i<startList.size(); i++){
				int wantFind = startList.get(i);
				for(int j=0; j<lEi.size(); j++){
					if(wantFind == lEi.get(j).EiStart){
						result.add(lEi.get(j).EiEnd);
					}
				}
			}
//		}catch (Exception ex) {
//			System.out.println(ex.getMessage());
//		}

		return result;
	}
	
	// 获得所有迭代泛化的点
	public List<Integer> getAllDirectGenNode(int ciID){
		List<Integer> result = new ArrayList<Integer>();
		List<Integer> startList = new ArrayList<Integer>();
		startList.add(ciID);
		while(this.getDirectGenNode(startList).size()!=0){
			List<Integer> directGenNode = this.getDirectGenNode(startList);
			for(int i=0; i<directGenNode.size(); i++){
				result.add(directGenNode.get(i));
			}
			startList = directGenNode;
		}
		return result;
	}
	
}
