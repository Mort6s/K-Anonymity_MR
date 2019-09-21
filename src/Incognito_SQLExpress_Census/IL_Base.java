package Incognito_SQLExpress_Census;

public class IL_Base {

	public String sAttrname;
	public String sClass;
	public int iCount;
	public double dMax;
	public double dMin;
	public double dRange;
	public double dHSub;
	public double dHAll;

	public IL_Base() {};
	
	public IL_Base(String _sAttrname, String _sClass, int _iCount, double _dMax, double _dMin, double _dRange){
		this.sAttrname = _sAttrname;
		this.sClass = _sClass;
		this.iCount = _iCount;
		this.dMax = _dMax;
		this.dMin = _dMin;
		this.dRange = _dRange;
	}
	
	public IL_Base(String _sAttrname, String _sClass, int _iCount, double _dHSub, double _dHAll){
		this.sAttrname = _sAttrname;
		this.sClass = _sClass;
		this.iCount = _iCount;
		this.dHSub = _dHSub;
		this.dHAll = _dHAll;
	}	
}
