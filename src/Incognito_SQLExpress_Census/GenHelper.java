package Incognito_SQLExpress_Census;

import java.util.List;
import java.util.Map;

public class GenHelper {

    GenTableDefinition_census_income gtd = new GenTableDefinition_census_income();
    GenTable gt = gtd.getGenTable(); 
	
	public GenHelper(){
		
	}

	public DataColumn getGenDataColumn(DataColumn dc,int genlevel){
		DataColumn genDataCol = new DataColumn();
		genDataCol.setColumnName(dc.getColumnName());
		GenColumn gc = gt.getGenColumn(dc.columnName);
		switch(gc.attr_class){
			case "Categorical":
				for (int i=1;i<=dc.size();i++) {
					String dcValue = dc.getValue(i).toLowerCase();
					for(GenBase gb:gc.lgb){
						if(genlevel == 0){
							genDataCol.add2ColumnList(dcValue);
							break;
						}else if(genlevel == gb.gen_level && dcValue.equals(gb.original_value.toLowerCase())){
							genDataCol.add2ColumnList(gb.gen_value);
							break;
						}						
					}
				};
				break;
			case "Numerical":
				for (int i=1;i<=dc.size();i++) {
					int dcv = Integer.parseInt(dc.getValue(i));
					for(GenBase gb:gc.lgb){
						if(genlevel == 0){
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
			System.out.println("Attribute " + dc.columnName + "'s Level "+ String.valueOf(genlevel) + " generalization rules are not complete, can not cover all of the properties");
			return genDataCol;
		}		
	}
	
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

	public DataTable getGenDataTable(DataTable dt, List<Integer> lGenGroup){
		DataTable dt_gen = new DataTable();
		for(int i = 0; i < lGenGroup.size(); i++) {
			DataColumn dc_gen = getGenDataColumn(dt.dataT.get(i), lGenGroup.get(i));
			dt_gen.addColumn(dc_gen);
		}
		return dt_gen;
	}
	
	public GenAttrLevel getAttrLevel(String attr){
		GenAttrLevel gal = new GenAttrLevel();
		GenColumn gc = gt.getGenColumn(attr);
		gal.setAttr(gc.attr_name);
		gal.setLevel_Item(gc.getLevelItem());
		return gal;
	}
	
	public int getMaxAttrLevelbyAttrName(String attr){
		if(gt.lgc.get(0).genLevelItem.size() == 0){
			gt.AutoCreateGenItem();
		}
		
		return gt.getGenColumn(attr).maxGenLevel;
	}		
}
