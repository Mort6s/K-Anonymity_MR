package OLA;

public class IL_Base {

	public String sAttrname;  // 属性名
	public String sClass;   // 如果是Numerical表示数值型，如果是Categorical表示分类型
	public int iCount;      // 数值型和分类型都用到
	public double dMax;     // 数值型用到
	public double dMin;     // 数值型用到
	public double dRange;   // 数值型用到
	public double dHSub;    // 分类型用到，具有最小公共祖先的分类子树的高度
	public double dHAll;    // 分类型用到
	
	// 构造函数
	public IL_Base() {};
	
	// 构造函数，用于数值型
	public IL_Base(String _sAttrname, String _sClass, int _iCount, double _dMax, double _dMin, double _dRange){
		this.sAttrname = _sAttrname;
		this.sClass = _sClass;
		this.iCount = _iCount;
		this.dMax = _dMax;
		this.dMin = _dMin;
		this.dRange = _dRange;
	}
	
	// 构造函数，用于分类型
	public IL_Base(String _sAttrname, String _sClass, int _iCount, double _dHSub, double _dHAll){
		this.sAttrname = _sAttrname;
		this.sClass = _sClass;
		this.iCount = _iCount;
		this.dHSub = _dHSub;
		this.dHAll = _dHAll;
	}
	
	
}
