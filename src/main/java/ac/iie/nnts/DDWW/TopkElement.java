package ac.iie.nnts.DDWW;

public class TopkElement {
	private long timestamp; //meaning
	private int key;//数据项的key
	private int sim;//相似性
	public TopkElement(long TS,int key,int sim)
	 {
	     this.timestamp = TS;
	     this.key = key;
	     this.sim = sim;
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

	public int getSim() {
		return sim;
	}

	public void setSim(int sim) {
		this.sim = sim;
	}
	
}
