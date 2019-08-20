/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ac.iie.nnts.LSH;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author Luan
 */
public class MesureMemoryThread extends Thread {

    public boolean stop = false;
    public static long maxMemory = 0;
    public  double averageTime = 0;

    public void computeMemory() {
        Runtime.getRuntime().gc();
        long used = Runtime.getRuntime().totalMemory()- Runtime.getRuntime().freeMemory();
        if(maxMemory < used)
            maxMemory = used;
    }

    @Override
    public void run() {
        while (true) {
            computeMemory();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Logger.getLogger(MesureMemoryThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public void writeResult() {

        System.out.println("Peak memory: " + maxMemory * 1.0 / 1024 / 1024);
        System.out.println("Average CPU time: " + averageTime);
    }
    
}
