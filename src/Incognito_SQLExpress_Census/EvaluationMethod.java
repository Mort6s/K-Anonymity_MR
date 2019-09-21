package Incognito_SQLExpress_Census;

import java.util.List;

public class EvaluationMethod {

	public EvaluationMethod() {};
	
	public int DMast(List<Integer> li) {
		int result = 0;
		for (int i = 0; i < li.size(); i++){
			result = result + li.get(i)*li.get(i);
		}
		return result;
	}
	
	public double IL(IL_Table ilt) {
		double result = 0.0;
		for (int i = 0; i < ilt.lcc.size(); i ++) { 
			IL_Column ilc_now = ilt.lcc.get(i);
			for (int j = 0; j < ilc_now.lil.size(); j++) {
				IL_Base ilb_now = ilc_now.lil.get(j);
				if (ilb_now.sClass.equals("Numerical")) {
					result = result + ilb_now.iCount*(ilb_now.dMax - ilb_now.dMin)/ilb_now.dRange;
				}else if(ilb_now.sClass.equals("Categorical")) {
					result = result + ilb_now.iCount*ilb_now.dHSub/ilb_now.dHAll;
				}				
			}
		}
		return result;
	}	
}
