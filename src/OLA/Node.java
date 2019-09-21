package OLA;

import java.util.ArrayList;
import java.util.List;

// 表示泛化格中的节点
public class Node {

	int tag = 0;    // 默认值为0。 tag为1表示满足k匿名，tag为2表示不满足k匿名。
	int iGenLatLevel;   // 表示在泛化格中的级别，全称是：Generalization Lattice Level
	List<Integer> liGenCom = new ArrayList<Integer>();    // 表示泛化组合(GenCom表示Generalization Combination)
	
	
	
}
