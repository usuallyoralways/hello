package ac.iie.nnts.DDWW;

public class Element
{
	private long timestamp; //meaning
	private int value; //meaning;
	//#######
	private int key;//数据项的key
	//######
	private long total;
	public Element(long TS,int key,int currentVal,long currentTotal)
	 {
	     this.timestamp = TS;
	     this.key = key;
	     this.value = currentVal ;
	     this.total = currentTotal;
	 }

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public int getValue() {
		return value;
	}
	
	public int getKey() {
		return key;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}
	
	public void setKey(int key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "Element{" +
				"timestamp=" + timestamp +
				", value=" + value +
				", key=" + key +
				", total=" + total +
				'}';
	}
}



