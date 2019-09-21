package OLA;

import java.util.Arrays;
import java.util.List;

public class GenTableDefinition_census_income {

	
	public GenTableDefinition_census_income(){};
	
	
	public GenTable getGenTable(){
	    
        GenTable gt = new GenTable();
        GenColumn gc = new GenColumn();
        
        gc.attr_name = "age";
        gc.attr_class = "Numerical";
        gc.dRange = 100.0;
        gc.addGenRule(0, 0, 0, "0");
        gc.addGenRule(1, 0, 5, "[0,5]");
        gc.addGenRule(1, 5, 10,"[5,10]");
        gc.addGenRule(1, 10, 15,"[10,15]");
        gc.addGenRule(1, 15, 20,"[15,20]");
        gc.addGenRule(1, 20, 25,"[20,25]");
        gc.addGenRule(1, 25, 30,"[25,30]");
        gc.addGenRule(1, 30, 35,"[30,35]");
        gc.addGenRule(1, 35, 40,"[35,40]");
        gc.addGenRule(1, 40, 45,"[40,45]");
        gc.addGenRule(1, 45, 50,"[45,50]");
        gc.addGenRule(1, 50, 55,"[50,55]");
        gc.addGenRule(1, 55, 60,"[55,60]");
        gc.addGenRule(1, 60, 65,"[60,65]");
        gc.addGenRule(1, 65, 70,"[65,70]");
        gc.addGenRule(1, 70, 75,"[70,75]");
        gc.addGenRule(1, 75, 80,"[75,80]");
        gc.addGenRule(1, 80, 85,"[80,85]");
        gc.addGenRule(1, 85, 90,"[85,90]");
        gc.addGenRule(1, 90, 95,"[90,95]");
        gc.addGenRule(1, 95, 100,"[95,100]");
        gc.addGenRule(2, 0, 10,"[0,10]");
        gc.addGenRule(2, 10, 20,"[10,20]");
        gc.addGenRule(2, 20, 30,"[20,30]");
        gc.addGenRule(2, 30, 40,"[30,40]");
        gc.addGenRule(2, 40, 50,"[40,50]");
        gc.addGenRule(2, 50, 60,"[50,60]");
        gc.addGenRule(2, 60, 70,"[60,70]");
        gc.addGenRule(2, 70, 80,"[70,80]");
        gc.addGenRule(2, 80, 90,"[80,90]");
        gc.addGenRule(2, 90, 100,"[90,100]");
        gc.addGenRule(3, 0, 20,"[0,20]");
        gc.addGenRule(3, 20, 40,"[20,40]");
        gc.addGenRule(3, 40, 60,"[40,60]");
        gc.addGenRule(3, 60, 80,"[60,80]");
        gc.addGenRule(3, 80, 100,"[80,100]");
        gc.addGenRule(4, 0, 100,"[0,100]");
//        gc.addGenRule(4, 0, 25,"[0,25]");
//        gc.addGenRule(4, 25,50,"[26,50]");
//        gc.addGenRule(4, 50, 75,"[51,75]");
//        gc.addGenRule(4, 75, 100,"[76,100]");
//        gc.addGenRule(5, 0, 50,"[0,50]");
//        gc.addGenRule(5, 50, 100,"[51,100]");
//        gc.addGenRule(4, 0, 100,"[0,100]");
        gt.addGenColumn(gc);

        gc = new GenColumn();
        gc.attr_name = "education";
        gc.attr_class = "Categorical";
        gc.maxGenLevel = 3;
        gc.addGenRule(0, "0", "0");
        gc.addGenRule(1, "Children", "Preschool");
        gc.addGenRule(1, "Less than 1st grade", "Preschool");
        gc.addGenRule(1, "1st 2nd 3rd or 4th grade", "1st-4th");
        gc.addGenRule(1, "5th or 6th grade", "5th-6th");
        gc.addGenRule(1, "7th and 8th grade", "Junior Sec.");
        gc.addGenRule(1, "9th grade", "Junior Sec.");
        gc.addGenRule(1, "10th grade", "Senior Sec.");
        gc.addGenRule(1, "11th grade", "Senior Sec.");
        gc.addGenRule(1, "12th grade no diploma", "Senior Sec.");
        gc.addGenRule(1, "Bachelors degree(BA AB BS)", "Bachelors");
        gc.addGenRule(1, "Masters degree(MA MS MEng MEd MSW MBA)", "HS-grad");
        gc.addGenRule(1, "High school graduate", "HS-grad");
        gc.addGenRule(1, "Doctorate degree(PhD EdD)", "HS-grad");
        gc.addGenRule(1, "Some college but no degree", "Some-college");
        gc.addGenRule(1, "Prof school degree (MD DDS DVM LLB JD)", "Prof-school");
        gc.addGenRule(1, "Associates degree-academic program", "Assoc-acdm");
        gc.addGenRule(1, "Associates degree-occup /vocational", "Assoc-voc"); 
        gc.addGenRule(2, "Children", "Preschool");
        gc.addGenRule(2, "Less than 1st grade", "Preschool");
        gc.addGenRule(2, "1st 2nd 3rd or 4th grade", "Primary");
        gc.addGenRule(2, "5th or 6th grade", "Primary");
        gc.addGenRule(2, "7th and 8th grade", "Secondary");
        gc.addGenRule(2, "9th grade", "Secondary");
        gc.addGenRule(2, "10th grade", "Secondary");
        gc.addGenRule(2, "11th grade", "Secondary");
        gc.addGenRule(2, "12th grade no diploma", "Secondary");
        gc.addGenRule(2, "Bachelors degree(BA AB BS)", "Some-college");
        gc.addGenRule(2, "Masters degree(MA MS MEng MEd MSW MBA)", "Some-college");
        gc.addGenRule(2, "High school graduate", "Some-college");
        gc.addGenRule(2, "Doctorate degree(PhD EdD)", "Some-college");
        gc.addGenRule(2, "Some college but no degree", "Some-college");
        gc.addGenRule(2, "Prof school degree (MD DDS DVM LLB JD)", "Prof-school");
        gc.addGenRule(2, "Associates degree-academic program", "Assoc");
        gc.addGenRule(2, "Associates degree-occup /vocational", "Assoc");   
        gc.addGenRule(3, "Children", "Education");
        gc.addGenRule(3, "Less than 1st grade", "Education");
        gc.addGenRule(3, "1st 2nd 3rd or 4th grade", "Education");
        gc.addGenRule(3, "5th or 6th grade", "Education");
        gc.addGenRule(3, "7th and 8th grade", "Education");
        gc.addGenRule(3, "9th grade", "Education");
        gc.addGenRule(3, "10th grade", "Education");
        gc.addGenRule(3, "11th grade", "Education");
        gc.addGenRule(3, "12th grade no diploma", "Education");
        gc.addGenRule(3, "Bachelors degree(BA AB BS)", "Education");
        gc.addGenRule(3, "Masters degree(MA MS MEng MEd MSW MBA)", "Education");
        gc.addGenRule(3, "High school graduate", "Education");
        gc.addGenRule(3, "Doctorate degree(PhD EdD)", "Education");
        gc.addGenRule(3, "Some college but no degree", "Education");
        gc.addGenRule(3, "Prof school degree (MD DDS DVM LLB JD)", "Education");
        gc.addGenRule(3, "Associates degree-academic program", "Education");
        gc.addGenRule(3, "Associates degree-occup /vocational", "Education");  
        gt.addGenColumn(gc);
        
        gc = new GenColumn();
        gc.attr_name = "class_of_worker";
        gc.attr_class = "Categorical";
        gc.maxGenLevel = 2;
        gc.addGenRule(0, "0", "0");
        gc.addGenRule(1, "Not in universe", "Not in universe");
        gc.addGenRule(1, "Self-employed-incorporated", "Self Employed");
        gc.addGenRule(1, "Self-employed-not incorporated", "Self Employed");
        gc.addGenRule(1, "Federal government", "Govement");
        gc.addGenRule(1, "State government", "Govement");
        gc.addGenRule(1, "Local government", "Govement");
        gc.addGenRule(1, "Without pay", "Unemployed");
        gc.addGenRule(1, "Never worked", "Unemployed");
        gc.addGenRule(1, "Private", "Private");
        gc.addGenRule(2, "Not in universe", "Work Class");
        gc.addGenRule(2, "Self-employed-incorporated", "Work Class");
        gc.addGenRule(2, "Self-employed-not incorporated", "Work Class");
        gc.addGenRule(2, "Federal government", "Work Class");
        gc.addGenRule(2, "State government", "Work Class");
        gc.addGenRule(2, "Local government", "Work Class");
        gc.addGenRule(2, "Without pay", "Work Class");
        gc.addGenRule(2, "Never worked", "Work Class");
        gc.addGenRule(2, "Private", "Work Class");
        gt.addGenColumn(gc);
        
        gc = new GenColumn();
        gc.attr_name = "race";
        gc.attr_class = "Categorical";
        gc.maxGenLevel = 1;
        gc.addGenRule(0, "0", "0");
        gc.addGenRule(1, "Amer Indian Aleut or Eskimo", "Race");
        gc.addGenRule(1, "Asian or Pacific Islander", "Race");
        gc.addGenRule(1, "Black", "Race");
        gc.addGenRule(1, "White", "Race");
        gc.addGenRule(1, "Other", "Race");
        gt.addGenColumn(gc);
        
        gc = new GenColumn();
        gc.attr_name = "marital_status";
        gc.attr_class = "Categorical";
        gc.maxGenLevel = 2;
        gc.addGenRule(0, "0", "0");
        gc.addGenRule(1, "Married-A F spouse present", "Married");
        gc.addGenRule(1, "Married-civilian spouse present", "Married");
        gc.addGenRule(1, "Married-spouse absent", "Married");
        gc.addGenRule(1, "Divorced", "Married");
        gc.addGenRule(1, "Widowed", "Married");
        gc.addGenRule(1, "Separated", "Married");
        gc.addGenRule(1, "Never married", "Never-married"); 
        gc.addGenRule(2, "Married-A F spouse present", "Marial Status");
        gc.addGenRule(2, "Married-civilian spouse present", "Marial Status");
        gc.addGenRule(2, "Married-spouse absent", "Marial Status");
        gc.addGenRule(2, "Divorced", "Marial Status");
        gc.addGenRule(2, "Widowed", "Marial Status");
        gc.addGenRule(2, "Separated", "Marial Status");
        gc.addGenRule(2, "Never married", "Marial Status");
        gt.addGenColumn(gc);
        
        gc = new GenColumn();
        gc.attr_name = "sex";
        gc.attr_class = "Categorical";
        gc.maxGenLevel = 1;
        gc.addGenRule(0, "0", "0");
        gc.addGenRule(1, "Male", "Person");
        gc.addGenRule(1, "Female", "Person");
        gt.addGenColumn(gc);
        
        gc = new GenColumn();
        gc.attr_name = "country_of_birth_self";
        gc.attr_class = "Categorical";
        gc.maxGenLevel = 2;
        gc.addGenRule(0, "0", "0");
        gc.addGenRule(1, "Canada", "NA");
        gc.addGenRule(1, "Puerto-Rico", "NA");
        gc.addGenRule(1, "Outlying-U S (Guam USVI etc)", "NA");
        gc.addGenRule(1, "United-States", "NA");
        gc.addGenRule(1, "Dominican-Republic", "NA");       
        gc.addGenRule(1, "El-Salvador", "CA");
        gc.addGenRule(1, "Mexico", "CA");
        gc.addGenRule(1, "Trinadad&Tobago", "CA");
        gc.addGenRule(1, "Haiti", "CA");
        gc.addGenRule(1, "Columbia", "SA");
        gc.addGenRule(1, "Guatemala", "SA");
        gc.addGenRule(1, "Honduras", "SA");
        gc.addGenRule(1, "Jamaica", "SA");
        gc.addGenRule(1, "Cuba", "SA");
        gc.addGenRule(1, "Ecuador", "SA");
        gc.addGenRule(1, "Nicaragua", "SA");
        gc.addGenRule(1, "Peru", "SA");
        gc.addGenRule(1, "Vietnam", "Asia");
        gc.addGenRule(1, "Philippines", "Asia");
        gc.addGenRule(1, "Laos", "Asia");
        gc.addGenRule(1, "China", "Asia");
        gc.addGenRule(1, "Hong Kong", "Asia");
        gc.addGenRule(1, "South Korea", "Asia");
        gc.addGenRule(1, "Thailand", "Asia");
        gc.addGenRule(1, "India", "Asia");
        gc.addGenRule(1, "Taiwan", "Asia");
        gc.addGenRule(1, "Japan", "Asia");
        gc.addGenRule(1, "Cambodia", "Asia");
        gc.addGenRule(1, "India", "Asia");
        gc.addGenRule(1, "Iran", "Asia");
        gc.addGenRule(1, "Scotland", "Euro");
        gc.addGenRule(1, "Italy", "Euro");
        gc.addGenRule(1, "Germany", "Euro");
        gc.addGenRule(1, "England", "Euro");
        gc.addGenRule(1, "Hungary", "Euro");
        gc.addGenRule(1, "Poland", "Euro");
        gc.addGenRule(1, "Ireland", "Euro");
        gc.addGenRule(1, "France", "Euro");
        gc.addGenRule(1, "Greece", "Euro");
        gc.addGenRule(1, "Yugoslavia", "Euro");
        gc.addGenRule(1, "Portugal", "Euro"); 
        gc.addGenRule(2, "Canada", "Global");
        gc.addGenRule(2, "Puerto-Rico", "Global");
        gc.addGenRule(2, "Outlying-U S (Guam USVI etc)", "Global");
        gc.addGenRule(2, "United-States", "Global");
        gc.addGenRule(2, "Dominican-Republic", "Global");       
        gc.addGenRule(2, "El-Salvador", "Global");
        gc.addGenRule(2, "Mexico", "Global");
        gc.addGenRule(2, "Trinadad&Tobago", "Global");
        gc.addGenRule(2, "Haiti", "Global");
        gc.addGenRule(2, "Columbia", "Global");
        gc.addGenRule(2, "Guatemala", "Global");
        gc.addGenRule(2, "Honduras", "Global");
        gc.addGenRule(2, "Jamaica", "Global");
        gc.addGenRule(2, "Cuba", "Global");
        gc.addGenRule(2, "Ecuador", "Global");
        gc.addGenRule(2, "Nicaragua", "Global");
        gc.addGenRule(2, "Peru", "Global");
        gc.addGenRule(2, "Vietnam", "Global");
        gc.addGenRule(2, "Philippines", "Global");
        gc.addGenRule(2, "Laos", "Global");
        gc.addGenRule(2, "China", "Global");
        gc.addGenRule(2, "Hong Kong", "Global");
        gc.addGenRule(2, "South Korea", "Global");
        gc.addGenRule(2, "Thailand", "Global");
        gc.addGenRule(2, "India", "Global");
        gc.addGenRule(2, "Taiwan", "Global");
        gc.addGenRule(2, "Japan", "Global");
        gc.addGenRule(2, "Cambodia", "Global");
        gc.addGenRule(2, "India", "Global");
        gc.addGenRule(2, "Iran", "Global");
        gc.addGenRule(2, "Scotland", "Global");
        gc.addGenRule(2, "Italy", "Global");
        gc.addGenRule(2, "Germany", "Global");
        gc.addGenRule(2, "England", "Global");
        gc.addGenRule(2, "Hungary", "Global");
        gc.addGenRule(2, "Poland", "Global");
        gc.addGenRule(2, "Ireland", "Global");
        gc.addGenRule(2, "France", "Global");
        gc.addGenRule(2, "Greece", "Global");
        gc.addGenRule(2, "Yugoslavia", "Global");
        gc.addGenRule(2, "Portugal", "Global"); 
        gt.addGenColumn(gc);

//        gc = new GenColumn();
//        gc.attr_name = "hours_per_week";
//        gc.attr_class = "Numerical";
//        gc.dRange = 100.0;
//        gc.addGenRule(0, 0, 0, "0");
//        gc.addGenRule(1, 0, 5, "[1,5]");
//        gc.addGenRule(1, 5, 10,"[6,10]");
//        gc.addGenRule(1, 10, 15,"[11,15]");
//        gc.addGenRule(1, 15, 20,"[16,20]");
//        gc.addGenRule(1, 20, 25,"[21,25]");
//        gc.addGenRule(1, 25, 30,"[26,30]");
//        gc.addGenRule(1, 30, 35,"[31,35]");
//        gc.addGenRule(1, 35, 40,"[36,40]");
//        gc.addGenRule(1, 40, 45,"[41,45]");
//        gc.addGenRule(1, 45, 50,"[46,50]");
//        gc.addGenRule(1, 50, 55,"[51,55]");
//        gc.addGenRule(1, 55, 60,"[56,60]");
//        gc.addGenRule(1, 60, 65,"[61,65]");
//        gc.addGenRule(1, 65, 70,"[66,70]");
//        gc.addGenRule(1, 70, 75,"[71,75]");
//        gc.addGenRule(1, 75, 80,"[76,80]");
//        gc.addGenRule(1, 80, 85,"[81,85]");
//        gc.addGenRule(1, 85, 90,"[86,90]");
//        gc.addGenRule(1, 90, 95,"[91,95]");
//        gc.addGenRule(1, 95, 100,"[96,100]");
//        gc.addGenRule(2, 0, 10,"[1,10]");
//        gc.addGenRule(2, 10, 20,"[11,20]");
//        gc.addGenRule(2, 20, 30,"[21,30]");
//        gc.addGenRule(2, 30, 40,"[31,40]");
//        gc.addGenRule(2, 40, 50,"[41,50]");
//        gc.addGenRule(2, 50, 60,"[51,60]");
//        gc.addGenRule(2, 60, 70,"[61,70]");
//        gc.addGenRule(2, 70, 80,"[71,80]");
//        gc.addGenRule(2, 80, 90,"[81,90]");
//        gc.addGenRule(2, 90, 100,"[91,100]");
//        gc.addGenRule(3, 0, 20,"[1,20]");
//        gc.addGenRule(3, 20, 40,"[21,40]");
//        gc.addGenRule(3, 40, 60,"[41,60]");
//        gc.addGenRule(3, 60, 80,"[61,80]");
//        gc.addGenRule(3, 80, 100,"[81,100]");
//        gc.addGenRule(4, 0, 100,"[1,100]");
//        gt.addGenColumn(gc);
        
//        gc = new GenColumn();
//        gc.attr_name = "occupation";
//        gc.attr_class = "Categorical";
//        gc.maxGenLevel = 1;
//        gc.addGenRule(0, "0", "0");
//        gc.addGenRule(1, "Prof-specialty", "occupation");
//        gc.addGenRule(1, "Transport-moving", "occupation");
//        gc.addGenRule(1, "Craft-repair", "occupation");
//        gc.addGenRule(1, "Tech-support", "occupation");
//        gc.addGenRule(1, "Sales", "occupation");
//        gc.addGenRule(1, "Priv-house-serv", "occupation");
//        gc.addGenRule(1, "Other-service", "occupation"); 
//        gc.addGenRule(1, "Farming-fishing", "occupation");
//        gc.addGenRule(1, "Handlers-cleaners", "occupation");
//        gc.addGenRule(1, "Exec-managerial", "occupation");
//        gc.addGenRule(1, "Armed-Forces", "occupation");
//        gc.addGenRule(1, "Machine-op-inspct", "occupation");
//        gc.addGenRule(1, "Adm-clerical", "occupation");
//        gc.addGenRule(1, "Protective-serv", "occupation");
//        gt.addGenColumn(gc);

        
        return gt;
		
	}

	public int getAttrRangeOrGenLevel(String sAttr){
		List<String> attQ = Arrays.asList(new String[]{"class_of_worker","education","marital_status","race","sex","country_of_birth_self","age","hours_per_week"});
		int[] attQ_RangeOrGenLevel = {2,3,2,1,1,2,100,100};
		int i = 0;
		for (i = 0; i < attQ.size(); i++){
			if (attQ.get(i).equals(sAttr)) {
				break;
			}
		}
		return attQ_RangeOrGenLevel[i];
	}
	
	public int getAttrRangeInGenLevel(String sAttr, int iLevel) {
		if(sAttr == "age" && iLevel == 0) {
			return 0;
		}else if(sAttr == "age" && iLevel == 1) {
			return 5;
		}else if(sAttr == "age" && iLevel == 2) {
			return 10;
		}else if(sAttr == "age" && iLevel == 3) {
			return 20;
		}else if(sAttr == "age" && iLevel == 4) {
			return 100;
		}
		else if(sAttr == "hours_per_week" && iLevel == 1) {
			return 5;
		}else if(sAttr == "hours_per_week" && iLevel == 2) {
			return 10;
		}else if(sAttr == "hours_per_week" && iLevel == 3) {
			return 20;
		}else if(sAttr == "hours_per_week" && iLevel == 4) {
			return 100;
		}

		return 0;
	}
	

}
