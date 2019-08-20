package ac.iie.nnts.Stream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;

public class Stream0 {

    public  LinkedList<Data> streams;

    public  Stream0() {
    	streams = new LinkedList<>();
    }

    public int getData(String filename) {
        try {
            BufferedReader bfr = new BufferedReader(new FileReader(new File(filename)));
            String line = "";
            int time = 1;//毫秒
            try {
                while ((line = bfr.readLine()) != null) {
                    String[] atts = line.split(",");
//                	String[] atts = line.split("\\|");
                    double[] d = new double[atts.length-1];//第一位是key，后面是属性值
                    for (int i = 1; i < d.length; i++) {
                        d[i] = Double.valueOf(atts[i]) + (new Random()).nextDouble() / 10000000;
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
