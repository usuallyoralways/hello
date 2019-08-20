package ac.iie.nnts.Stream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

public class Stream1 {

    public  LinkedList<Data> streams;

    public  Stream1() {
    	streams = new LinkedList<>();
    }

    public int getData(String filename) {
        try {
            BufferedReader bfr = new BufferedReader(new FileReader(new File(filename)));
            String line = "";
            int time = 1;//毫秒      
            ArrayList<double[]> minMax = new ArrayList<>();
            try {
                while ((line = bfr.readLine()) != null) {
                    String[] atts = line.split(",");
                    double [] d = new double[atts.length-1];//第一位是key，后面是属性值
                    
                    //最大最小值更新和规范化计算Z-Score
                    for (int i = 1; i <= d.length; i++) {
                    	double att=Double.valueOf(atts[i]);
                    	double[] mm;
                    	if(minMax.size()<d.length) {
                    		mm = new double[2];
                			mm[0]=att;
                			mm[1]=att*att;
                			minMax.add(mm);
                    	}else {
	                    	mm =minMax.get(i-1);
	            			mm[0]=(mm[0]*(time-1)+att)/time;
	            			mm[1]=(mm[1]*(time-1)+att*att)/time;
                    	}
                    	double theta = Math.sqrt(Math.abs(mm[1]-mm[0]*mm[0]));
                        d[i-1] = (theta==0)?mm[0]:(att-mm[0])/theta;
                    }
                    Data data = new Data(Integer.valueOf(atts[0]),d,time);
                    streams.add(data);
                    time++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return streams.peek().values.length;
    }
}
