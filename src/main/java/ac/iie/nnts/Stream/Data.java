package ac.iie.nnts.Stream;

import java.sql.Timestamp;

public class Data {
	
	public double[] values;
	public int key;
	public int arrivalTime;
	public Timestamp time;
	
	public Data() {
	}
	
	public Data(int key,double[] values,  int arrivalTime,Timestamp timestamp) {
		this.key = key;
		this.values = values;
		this.arrivalTime = arrivalTime;
		this.time =timestamp;
	}

	public Data(int key,double[] values,  int arrivalTime) {
		this.key = key;
		this.values = values;
		this.arrivalTime = arrivalTime;
	}

	@Override
	public String toString() {
		String re = "key: " + String.valueOf(key) + "  arrivalTime: " + String.valueOf(arrivalTime) +" Timestamp: "+time.toString()+" values: ";
		for (double item : values) {
			re += " ";
			re += String.valueOf(item);
		}
		re += "\n";
		return re;
	}
}