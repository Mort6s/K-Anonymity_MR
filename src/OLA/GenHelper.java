package OLA;

import java.util.List;
import java.util.Map;

public class GenHelper {

    GenTableDefinition_census_income gtd = new GenTableDefinition_census_income();   // 定义泛化表类
    GenTable gt = gtd.getGenTable();   // 调用类中的方法获得泛化表
	
	// 构造函数
	public GenHelper(){
		
	}

	// 根据属性获得泛化属性
	public DataColumn getGenDataColumn(DataColumn dc,int genlevel){
		DataColumn genDataCol = new DataColumn();	// 定义泛化列genDataCol作为输出
		genDataCol.setColumnName(dc.getColumnName());
		GenColumn gc = gt.getGenColumn(dc.columnName);
		switch(gc.attr_class){
			case "Categorical":
				for (int i=1;i<=dc.size();i++) {
					String dcValue = dc.getValue(i).toLowerCase();
					for(GenBase gb:gc.lgb){
						if(genlevel == 0){         // 如果泛化级别为0，表示不需要泛化
							genDataCol.add2ColumnList(dcValue);
							break;
						}else if(genlevel == gb.gen_level && dcValue.equals(gb.original_value.toLowerCase())){
//							System.out.print(i + "成功：");
//							System.out.print(dcValue);
//							System.out.println();
							genDataCol.add2ColumnList(gb.gen_value);
							break;
						}else{
//							System.out.print(i + "失败：");
//							System.out.print(gb.original_value.toLowerCase() + " ");
//							System.out.print(gb.gen_level + " ");
//							System.out.print(dc.getValue(i).toLowerCase() + " ");
//							System.out.println();
						}
						
					}
				};
				break;
			case "Numerical":
				for (int i=1;i<=dc.size();i++) {
					int dcv = Integer.parseInt(dc.getValue(i));
					for(GenBase gb:gc.lgb){
						if(genlevel == 0){         // 如果泛化级别为0，表示不需要泛化
							genDataCol.add2ColumnList(dc.getValue(i));
							break;
						}else if(genlevel == gb.gen_level && dcv >= gb.start_num && dcv < gb.end_num){
							genDataCol.add2ColumnList(gb.gen_value);
							break;
						}
					}
				}
				break;
		}
		if(dc.size() == genDataCol.size()){
			return genDataCol;
		}else{
			System.out.println("属性" + dc.columnName + "的"+ String.valueOf(genlevel) + "级别泛化规则不完整，不能覆盖全部属性");
			return genDataCol;
		}
		
	}
	
	

	// 根据属性获得泛化属性
		public DataTable getGenDataTable(DataTable dt,CiColumn ciC){
			DataTable dt_gen = new DataTable();
			for(Map<String,String> ms:ciC.columnList){
				for(Map.Entry<String,String> me : ms.entrySet()){
					DataColumn dc_pregen = dt.getColumn(me.getKey());
					DataColumn dc_gen = getGenDataColumn(dc_pregen,Integer.parseInt(me.getValue()));
					dt_gen.addColumn(dc_gen);
					
				}
			}
			return dt_gen;
		}

		
		// 根据泛化组合列表获得泛化表
		public DataTable getGenDataTable(DataTable dt, List<Integer> lGenGroup){
			DataTable dt_gen = new DataTable();
			for(int i = 0; i < lGenGroup.size(); i++) {
				DataColumn dc_gen = getGenDataColumn(dt.dataT.get(i), lGenGroup.get(i));
				dt_gen.addColumn(dc_gen);
			}
			return dt_gen;
		}
		
		
		// 获得属性和泛化级别
		public GenAttrLevel getAttrLevel(String attr){
			GenAttrLevel gal = new GenAttrLevel();
			GenColumn gc = gt.getGenColumn(attr);
			gal.setAttr(gc.attr_name);
			gal.setLevel_Item(gc.getLevelItem());
			return gal;
		}
		
		
		// 根据属性名称获得最大泛化级别
		public int getMaxAttrLevelbyAttrName(String attr){
			// (1) 判断genLevelItem是不是已经生成过了，如果为空则自动生成
			if(gt.lgc.get(0).genLevelItem.size() == 0){
				gt.AutoCreateGenItem();
			}
			
			// (2) 找到该属性的泛化列并返回
			return gt.getGenColumn(attr).maxGenLevel;
		}
		
}
