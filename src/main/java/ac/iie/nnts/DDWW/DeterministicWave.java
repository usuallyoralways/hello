package ac.iie.nnts.DDWW;

import ac.iie.nnts.Constant.Constant;

import java.util.LinkedList;

public class DeterministicWave {
	//#####不能是static,因为每个DW的情况不一样，必须是每个DW私有的数据
	//保存每一层因为容量不够二删除的元素
	private LinkedList<OverElement> overflow = new LinkedList<OverElement>();
	public LinkedList<OverElement> getOverflow() {
		return overflow;
	}
	//#########################################

	long MaxTSInSampleWindow = 0; // the biggest TS in the window
	long MinTSInSampleWindow = 0; // the smaller TS in the window
	
	long totalValue = 0L;
	
	private int expire = 50; //滑动窗口宽度
	
	public int getExpire() {
		return expire;
	}

	public void setExpire(int expire) {
		this.expire = expire;
	}

	private double currentRelativeError;
	
	private LinkedList<Element> RightQueue[] = null; //保存滑动窗口范围内的数据项
	private Element left = null; //保存最新过时的数据项
	private LinkedList<Element> AllInWindow = null;
	//#####不能是static,因为每个DW的情况不一样，必须是每个DW私有的数据
	public LinkedList<Element> getAllInWindow() {
		return AllInWindow;
	}
	
	public void Initialized( double relativeError,int expire)
	{
		this.setError(relativeError);
		this.setExpire(expire);
		overflow = new LinkedList<OverElement>();
		RightQueue = new LinkedList[getMaxLevelNum()];
		AllInWindow = new LinkedList<>();
		totalValue = 0L;
	}
	
	boolean isEmpty()
	{
		if(AllInWindow.isEmpty()) return true;
		else
			return false;
	}
	
	public void setError(double error)
	{
		currentRelativeError = error;
	}
			
	public double getError() 
	{
		return currentRelativeError;
	}
	public long getRank()
	{
		return totalValue;
	}
			
	public int getMaxLevelNum()
	{
		return 41;
	}
	
	public int getLimitedElementNumPerLevel()
	{
		int perLevelEleNum =(int)((1+ (1.0 / getError())/2)) ;//每层最多存储几个位置信息（时间戳）			
		return perLevelEleNum;
	}
	
	public long getMaxTimeStamp()
	{
		return MaxTSInSampleWindow;
	}
	
	public long getMinTimeStamp()
	{
		return MinTSInSampleWindow;
	}
	
	public static int getLevelNumber(long total, int value)//论文中有证明，快速获得要插入的层号
	{
		if(total + value == 0) return 0;
		
		long f = total;
		long g = total +(long)value ;
		
		long h = f^g;
		int levelNumber = (int) (Math.log(h)/Math.log(2));
		return levelNumber;
	}
	
	//input a number(value) and renew the queues
	public void input (int key, int value,  long timestamp) 
	{	
		//###删除overflow中过时数据
		while(!overflow.isEmpty()) {	
			OverElement oldEle  = overflow.getFirst();
			if(oldEle.getTimestamp() > timestamp-expire)
				break;
			else{//如果过时了
				overflow.removeFirst();
				Constant.n--;
			}
		}
		//###
		
		while(!AllInWindow.isEmpty()) {	
			Element oldEle  = AllInWindow.getFirst();
			if(oldEle.getTimestamp() > timestamp-expire)
				break;
			else{//如果过时了
				left = AllInWindow.removeFirst();//从L中删除，用left保存最新过时的元素
				Constant.n--;
				int todelete = getLevelNumber(left.getTotal()-left.getValue(),left.getValue());
				if(!RightQueue[todelete].isEmpty()) {//从对应层队列中删除
					RightQueue[todelete].removeFirst();
				}
			}
		}
		
		Element newEle = new Element(timestamp,key,value,totalValue+value);
		int levelNumber = getLevelNumber(totalValue,value);//获取要插入的层号			
		MaxTSInSampleWindow = timestamp;//修改当前最大时间戳为最新插入项时间戳			
		if(MinTSInSampleWindow == 0) {MinTSInSampleWindow = timestamp;}
		if(null==RightQueue[levelNumber])  //如果当前层未曾插入数据，先初始化
			RightQueue[levelNumber]=new LinkedList<>();			
		else if (RightQueue[levelNumber].size() >= getLimitedElementNumPerLevel())  //如果当前层数据已满
		{
			Element lastOld = RightQueue[levelNumber].removeFirst();//删除最旧的数据项
			//###########将溢出的数据项插入overflow
			overflow.addLast(new OverElement(lastOld.getTimestamp(),lastOld.getKey()));
			//将删除的超数量的数据从L中删除
			AllInWindow.remove(lastOld);	
		}
//		if(newEle.getKey()==1046)
//		System.out.println(this.hashCode()+"++ "+newEle.getKey()+"  "+levelNumber +" "+totalValue+" "+value);
		RightQueue[levelNumber].addLast(newEle);
		AllInWindow.addLast(newEle);
		Constant.n++;
		totalValue = totalValue + value;
	}
	
	public void setBiggerElement(Element inputel)
	{
		ValueType.biggerElement.setTimestamp(inputel.getTimestamp());
		ValueType.biggerElement.setTotal(inputel.getTotal());
		ValueType.biggerElement.setValue(inputel.getValue());
	}
	public void setSmallerElement(Element inputel)
	{
		ValueType.smallElement.setTimestamp(inputel.getTimestamp());
		ValueType.smallElement.setTotal(inputel.getTotal());
		ValueType.smallElement.setValue(inputel.getValue());
	}
	
	public long Query()
	{	
		if (left==null) {
			return totalValue;//没有过时数据，全部都在滑动窗口范围内
		}else{
			setSmallerElement(left);
		}

        if(AllInWindow.isEmpty()) {
        	return 0;
        }else {
        	setBiggerElement(AllInWindow.getFirst());
        }
        
		int v2=0;
		long x=0;
		
		v2 = ValueType.biggerElement.getValue();
		x = (ValueType.smallElement.getTotal() + ValueType.biggerElement.getTotal() -v2)/2;

		return totalValue -x;
	}
	
}