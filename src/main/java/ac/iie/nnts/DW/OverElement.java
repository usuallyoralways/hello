package ac.iie.nnts.DW;

public class OverElement
{
	private long timestamp; //meaning
	//#######
	private int key;//数据项的key
	//######
	public OverElement(long TS,int key)
	 {
	     this.timestamp = TS;
	     this.key = key;
	 }

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
		
	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "OverElement{" +
				"timestamp=" + timestamp +
				", key=" + key +
				'}';
	}
}



