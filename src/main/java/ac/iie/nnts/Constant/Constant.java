package ac.iie.nnts.Constant;


public class Constant {
	public static long curTime = 0;
	public static int flag = 0;//0是异常检测；1是MIPS
	//DW
	public static double relativeError = 0.1;//DW相对误差
	public static int expire = 1000; //窗口大小
	public static int K = 4;//数组每行几位，即哈希值的位数
	public static int L = 1;//数组有多少行，即哈希值的个数
	
	//阈值
	public static double alpha = 0.2;//阈值水平，小于窗口内数量的这个比例即为异常
	public static double mean =0;//平均值
	public static int n = 0;
    public static int DW_nums = 0;

	
	//文件路径
	public static String dataPath = "D:/Documents/vscode/data/tao.csv";
	public static String outputPath = "D:/Documents/vscode/data/tao_outliers_190731.txt";

	//topK
	public static int topK=20;
	public static double[] topkBase = {-9.99,84.01,23.56};
	public static String topKPath = "D:/Documents/vscode/data//tao_topk_190510.txt";
	public static String topKcoll = "D:/Documents/vscode/data//tao_key.txt";
	
//	public static Data base ;//基准向量
	public static int timeRange=2000;//哪个时间段的topk
}
