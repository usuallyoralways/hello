package ac.iie.nnts.DW;


import ac.iie.nnts.Constant.Constant;
import io.hops.exception.StorageException;

import java.util.LinkedList;

public class DeterministicWave {

    //??????
    public int parakey;//?
    public String paraTable;//?

    //#####???static,????DW????????????DW?????
    //?????????????????
//    private LinkedList<OverElement> overflow = new LinkedList<OverElement>();
    private String overflowPG = "overflow";

//    public LinkedList<OverElement> getOverflow() {
//        return overflow;
//    }
    public String getOverflowPG() {
        return overflowPG;
    }

    //#########################################

    long MaxTSInSampleWindow = 0; // the biggest TS in the window
    long MinTSInSampleWindow = 0; // the smaller TS in the window



    long totalValue = 0L;

    private int expire = 50; //??????

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) throws StorageException {
        PGFunctions.setExpire(paraTable,parakey,expire);
        this.expire = expire;
    }

    private double currentRelativeError;

//    private LinkedList<Element> RightQueue[] = null; //?????????????

    private String RightQueuePG ="RigthQueue";

    private Element left = null; //??????????
//    private LinkedList<Element> AllInWindow = null;

    private String AllInWindowPG = "AllInWindow";


    //#####???static,????DW????????????DW?????
//    public LinkedList<Element> getAllInWindow() {
//        return AllInWindow;
//    }


    public String getAllInWindowPG(){
        return AllInWindowPG;
    }




    public void Initialized( double relativeError,int expire) throws StorageException {

        PGFunctions.createParameter(paraTable,parakey);
        PGFunctions.setCurrentRelativeError(paraTable,parakey,relativeError);
        PGFunctions.setExpire(paraTable,parakey,expire);
        PGFunctions.newOverElement(overflowPG);
        PGFunctions.newElements(RightQueuePG,getMaxLevelNum());
        PGFunctions.newElement(AllInWindowPG);
        PGFunctions.setTotalValue(paraTable,parakey,0);

        this.setParaTable(paraTable);
        this.setParakey(parakey);

        this.setError(relativeError);
        this.setExpire(expire);
//        overflow = new LinkedList<OverElement>();
//        RightQueue = new LinkedList[getMaxLevelNum()];
//        AllInWindow = new LinkedList<>();
        totalValue = 0L;
    }

    public void Initialized( double relativeError,int expire,int parakey,String paraTable) throws StorageException {

        PGFunctions.createParameter(paraTable,parakey);
        PGFunctions.setCurrentRelativeError(paraTable,parakey,relativeError);
        PGFunctions.setExpire(paraTable,parakey,expire);
        PGFunctions.newOverElement(overflowPG);
        PGFunctions.newElements(RightQueuePG,getMaxLevelNum());
        PGFunctions.newElement(AllInWindowPG);
        PGFunctions.setTotalValue(paraTable,parakey,0);

        this.setParaTable(paraTable);
        this.setParakey(parakey);

        this.setError(relativeError);
        this.setExpire(expire);
//        overflow = new LinkedList<OverElement>();
//        RightQueue = new LinkedList[getMaxLevelNum()];
//        AllInWindow = new LinkedList<>();
        totalValue = 0L;
    }

    public void InitializedPG( double relativeError,int expire) throws StorageException {


        PGFunctions.createParameter(paraTable,parakey);
        PGFunctions.setCurrentRelativeError(paraTable,parakey,relativeError);
        PGFunctions.setExpire(paraTable,parakey,expire);
        PGFunctions.newOverElement(overflowPG);
        PGFunctions.newElements(RightQueuePG,getMaxLevelNum());
        PGFunctions.newElement(AllInWindowPG);
        PGFunctions.setTotalValue(paraTable,parakey,0);

        this.parakey=parakey;
        this.paraTable=paraTable;

        this.setError(relativeError);
        this.setExpire(expire);
//        overflow = new LinkedList<OverElement>();

        PGFunctions.newOverElement(overflowPG);

//        RightQueue = new LinkedList[getMaxLevelNum()];

        PGFunctions.newElements(RightQueuePG,getMaxLevelNum());

//        AllInWindow = new LinkedList<>();
        totalValue = 0L;
    }

    boolean isEmptyPG() throws StorageException {
        if(PGFunctions.isEmpty(AllInWindowPG)) return true;
        else
            return false;
    }

//    boolean isEmpty()
//    {
//        if(AllInWindow.isEmpty()) return true;
//        else
//            return false;
//    }

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

    public void setMaxLevelNum() throws StorageException {
        PGFunctions.setMaxLevelNum(paraTable,parakey,41);
    }

    public int getLimitedElementNumPerLevel()
    {
        int perLevelEleNum =(int)((1+ (1.0 / getError())/2)) ;//?????????????????
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

    public static int getLevelNumber(long total, int value)//?????????????????
    {
        if(total + value == 0) return 0;

        long f = total;
        long g = total +(long)value ;

        long h = f^g;
        int levelNumber = (int) (Math.log(h)/Math.log(2));
        return levelNumber;
    }

    public void input (int key, int value,  long timestamp) throws StorageException {
        //###??overflow?????
        while(!PGFunctions.isEmpty(overflowPG)) {
            OverElement oldEle  = PGFunctions.getFisrtOverElement(overflowPG);
            if(oldEle.getTimestamp() > timestamp-expire)
                break;
            else{//?????
//                overflow.removeFirst();
                long tmptimestamp= PGFunctions.getFisrt(overflowPG);
                PGFunctions.remove(overflowPG,tmptimestamp);
                Constant.n--;
            }
        }
        //###

//        while(!AllInWindow.isEmpty()) {
        while (!PGFunctions.isEmpty(AllInWindowPG)){
//            Element oldEle  = AllInWindow.getFirst();
            Element oldEle = PGFunctions.getFisrtElement(AllInWindowPG);

            if(oldEle.getTimestamp() > timestamp-expire)
                break;
            else{//?????
//                left = AllInWindow.removeFirst();//?L?????left?????????
                left = PGFunctions.getFisrtElement(AllInWindowPG);
                long tmptimestamp= PGFunctions.getFisrt(AllInWindowPG);
                PGFunctions.remove(AllInWindowPG,tmptimestamp);

                Constant.n--;
                int todelete = getLevelNumber(left.getTotal()-left.getValue(),left.getValue());
//                if(!RightQueue[todelete].isEmpty()) {//?????????
//                    RightQueue[todelete].removeFirst();
//                }
                if(!PGFunctions.isEmpty(RightQueuePG+String.valueOf(todelete))){
                    tmptimestamp= PGFunctions.getFisrt(RightQueuePG+String.valueOf(todelete));
                    PGFunctions.remove(RightQueuePG+String.valueOf(todelete),tmptimestamp);
                }
            }
        }

        Element newEle = new Element(timestamp,key,value,totalValue+value);
        int levelNumber = getLevelNumber(totalValue,value);//????????
        MaxTSInSampleWindow = timestamp;//??????????????????
        PGFunctions.setMaxTSInSampleWindow( paraTable,parakey,timestamp);

        if(MinTSInSampleWindow == 0) {MinTSInSampleWindow = timestamp;}
//        if(null==RightQueue[levelNumber])  //????????????????
//            RightQueue[levelNumber]=new LinkedList<>();

        if(PGFunctions.isEmpty(RightQueuePG+String.valueOf(levelNumber))){
            PGFunctions.newElement(RightQueuePG+String.valueOf(levelNumber));
        }
//
//        else if (RightQueue[levelNumber].size() >= getLimitedElementNumPerLevel())  //?????????
//        {
//            Element lastOld = RightQueue[levelNumber].removeFirst();//????????
//            //###########?????????overflow
//            overflow.addLast(new OverElement(lastOld.getTimestamp(),lastOld.getKey()));
//            //???????????L???
//            AllInWindow.remove(lastOld);
//        }

        else if (PGFunctions.getSize(RightQueuePG+String.valueOf(levelNumber))>getLimitedElementNumPerLevel())
        {
//            Element lastOld = RightQueue[levelNumber].removeFirst();//????????
            Element lastOld = PGFunctions.getFisrtElement(RightQueuePG+String.valueOf(levelNumber));
            long tmptimestamp= PGFunctions.getFisrt(RightQueuePG+String.valueOf(levelNumber));
            PGFunctions.remove(RightQueuePG+String.valueOf(levelNumber),tmptimestamp);
            //###########?????????overflow
//            overflow.addLast(new OverElement(lastOld.getTimestamp(),lastOld.getKey()));
            PGFunctions.insertOverElement(overflowPG,(new OverElement(lastOld.getTimestamp(),lastOld.getKey())));


            //???????????L???
//            AllInWindow.remove(lastOld);
            PGFunctions.remove(AllInWindowPG,lastOld.getTimestamp());
        }


//		if(newEle.getKey()==1046)
//		System.out.println(this.hashCode()+"++ "+newEle.getKey()+"  "+levelNumber +" "+totalValue+" "+value);
//        RightQueue[levelNumber].addLast(newEle);
        PGFunctions.insertElement(RightQueuePG+String.valueOf(levelNumber),newEle);
//        AllInWindow.addLast(newEle);
        PGFunctions.insertElement(AllInWindowPG,newEle);
        Constant.n++;
        totalValue = totalValue + value;
        PGFunctions.setTotalValue(paraTable,parakey,totalValue);
    }



//    //input a number(value) and renew the queues
//    public void input (int key, int value,  long timestamp)
//    {
//        //###??overflow?????
//        while(!overflow.isEmpty()) {
//            OverElement oldEle  = overflow.getFirst();
//            if(oldEle.getTimestamp() > timestamp-expire)
//                break;
//            else{//?????
//                overflow.removeFirst();
//                Constant.n--;
//            }
//        }
//        //###
//
//        while(!AllInWindow.isEmpty()) {
//            Element oldEle  = AllInWindow.getFirst();
//            if(oldEle.getTimestamp() > timestamp-expire)
//                break;
//            else{//?????
//                left = AllInWindow.removeFirst();//?L?????left?????????
//                Constant.n--;
//                int todelete = getLevelNumber(left.getTotal()-left.getValue(),left.getValue());
//                if(!RightQueue[todelete].isEmpty()) {//?????????
//                    RightQueue[todelete].removeFirst();
//                }
//            }
//        }
//
//        Element newEle = new Element(timestamp,key,value,totalValue+value);
//        int levelNumber = getLevelNumber(totalValue,value);//????????
//        MaxTSInSampleWindow = timestamp;//??????????????????
//        if(MinTSInSampleWindow == 0) {MinTSInSampleWindow = timestamp;}
//        if(null==RightQueue[levelNumber])  //????????????????
//            RightQueue[levelNumber]=new LinkedList<>();
//        else if (RightQueue[levelNumber].size() >= getLimitedElementNumPerLevel())  //?????????
//        {
//            Element lastOld = RightQueue[levelNumber].removeFirst();//????????
//            //###########?????????overflow
//            overflow.addLast(new OverElement(lastOld.getTimestamp(),lastOld.getKey()));
//            //???????????L???
//            AllInWindow.remove(lastOld);
//        }
////		if(newEle.getKey()==1046)
////		System.out.println(this.hashCode()+"++ "+newEle.getKey()+"  "+levelNumber +" "+totalValue+" "+value);
//        RightQueue[levelNumber].addLast(newEle);
//        AllInWindow.addLast(newEle);
//        Constant.n++;
//        totalValue = totalValue + value;
//    }

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

    public long Query() throws StorageException {
        left = PGFunctions.getLeft(paraTable,parakey);
        if (left==null) {
            return getTotalValue();//??????????????????
        }else{
            setSmallerElement(left);
        }

//        if(AllInWindow.isEmpty()) {
//            return 0;
//        }else {
//            setBiggerElement(AllInWindow.getFirst());
//        }

        if(PGFunctions.isEmpty(AllInWindowPG)) {
            return 0;
        }else {
            setBiggerElement(PGFunctions.getFisrtElement(AllInWindowPG));
        }

        int v2=0;
        long x=0;

        v2 = ValueType.biggerElement.getValue();
        x = (ValueType.smallElement.getTotal() + ValueType.biggerElement.getTotal() -v2)/2;

        return totalValue -x;
    }

//
//    public long Query()
//    {
//        if (left==null) {
//            return totalValue;//??????????????????
//        }else{
//            setSmallerElement(left);
//        }
//
//        if(AllInWindow.isEmpty()) {
//            return 0;
//        }else {
//            setBiggerElement(AllInWindow.getFirst());
//        }
//
//        int v2=0;
//        long x=0;
//
//        v2 = ValueType.biggerElement.getValue();
//        x = (ValueType.smallElement.getTotal() + ValueType.biggerElement.getTotal() -v2)/2;
//
//        return totalValue -x;
//    }


    public Element getLeft() throws StorageException {
        return PGFunctions.getLeft(paraTable,parakey);
    }

    public void setLeft(Element left) throws StorageException {
        this.left = left;
        PGFunctions.setLeft(paraTable,parakey,left);
    }

    public long getTotalValue() throws StorageException {
        totalValue=PGFunctions.getTotalValue(paraTable,parakey);
        return totalValue;
    }

    public void setTotalValue(long totalValue) throws StorageException {
        this.totalValue = totalValue;
        PGFunctions.setTotalValue(paraTable,parakey,totalValue);
    }

    public int getParakey() {
        return parakey;
    }

    public void setParakey(int parakey) {
        this.parakey = parakey;
    }

    public String getParaTable() {
        return paraTable;
    }

    public void setParaTable(String paraTable) {
        this.paraTable = paraTable;
    }

    public void setOverflowPG(String overflowPG) {
        this.overflowPG = overflowPG;
    }

    public String getRightQueuePG() {
        return RightQueuePG;
    }

    public void setRightQueuePG(String rightQueuePG) {
        RightQueuePG = rightQueuePG;
    }

    public void setAllInWindowPG(String allInWindowPG) {
        AllInWindowPG = allInWindowPG;
    }

    public LinkedList<Element> getAllInWindow() throws StorageException {
        return PGFunctions.getElements(AllInWindowPG);
    }

    public LinkedList<OverElement> getOverflow() throws StorageException {
        return PGFunctions.getOverElements(overflowPG);
    }



    public static void main(String[] args) throws StorageException, InterruptedException {
        DeterministicWave deterministicWavePG = new DeterministicWave();
        deterministicWavePG.Initialized(0,5000000,124,"para");
        for (int i=0;i<1000;i++){
            long timestamp =System.currentTimeMillis();
            int key = i;
            int value =i;
            deterministicWavePG.input(key,value,timestamp);
            Thread.sleep(1000);
        }
    }

}