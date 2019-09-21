package Incognito_SQLExpress_Census;

import java.util.Arrays;
import java.util.List;

public class public_data {

	public public_data() {};
	
	public static final int k = 100;
	public static final List<String> attQ = Arrays.asList(new String[]{"age","class_of_worker","education","marital_status","race","sex","country_of_birth_self"});
	public static final List<String> attQAll = Arrays.asList(new String[]{"age","class_of_worker","education","marital_status","race","sex","country_of_birth_self"});

	public static final String otherArgs[] = new String[]{
			"hdfs://master:9000/input/inputMPP_M/Paper2Test",
			"hdfs://master:9000/output/outputMPP_M/Paper2Test"};
	
	public static final int MR_Condition_Size = 0;
	public static final Integer totalNum = 95130;
	public static final double dSamPer = 0.01;
	public static final Integer AllCount = 95130;
}
