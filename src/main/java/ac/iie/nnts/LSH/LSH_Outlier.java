package ac.iie.nnts.LSH;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import ac.iie.nnts.Constant.Constant;
import ac.iie.nnts.Stream.Data;
import ac.iie.nnts.SignedRandomProjection.SignRandomProjection;
import ac.iie.nnts.Stream.Stream;
import io.hops.exception.StorageException;

public class LSH_Outlier {

	public static Stream dataStream;
	//####存储异常数据
	public static HashSet<Integer> outliers = null;
	public static HashSet<Integer> topK = null;
	//####
	public static void detectOutliers(String dataFile, double relativeError, int expire, int K,int L,double  alpha) throws StorageException {
		outliers = new HashSet<>();
		dataStream = new Stream();
		int dim = dataStream.getData(dataFile);//数据流准备好
		
		SignRandomProjection proj = new SignRandomProjection(dim, K,L);//哈希向量准备好
		//放在后面防止(1<<sample)<K,此时k这么大没有意义
		LSH_DW lshi = new LSH_DW(Constant.K,L,relativeError, expire);
		
		while(!dataStream.streams.isEmpty()) {
			Data data = new Data();
			data  = dataStream.streams.poll();
			int[][]  hashes = proj.getHash(data.values);
			lshi.add(hashes, data.arrivalTime,data.key);
		}	
	}
	
	public static void findMIPS(String dataFile, double relativeError, int expire, int K,int L,int topk) throws StorageException {
		topK = new HashSet<>();//存储topK候选
		dataStream = new Stream();
		int dim = dataStream.getData(dataFile);//待选数据流准备好
		LSH_topK lshi = new LSH_topK(K,L,relativeError, expire);
		SignRandomProjection proj = new SignRandomProjection(dim, K,L);//哈希向量准备好
		int[][]  hashes;
		int count=0;
		while(!dataStream.streams.isEmpty()) {
			Data data = new Data();
			data  = dataStream.streams.poll();
			hashes = proj.getHash(data.values);
			lshi.add(hashes, data.arrivalTime,data.key);
			Constant.curTime = data.arrivalTime;
			count++;
			if(count>5000)
				break;
		}
		
		hashes = proj.getHash(Constant.topkBase);//L行，每行的01情况
		lshi.Query(hashes,Constant.timeRange);
	}
	
	public static void write(String filename, List<Integer> outLiers) {
		try {
			File file = new File(filename);//结果文件
			FileOutputStream out = new FileOutputStream(file);
			OutputStreamWriter osw = new OutputStreamWriter(out,"UTF-8");
			BufferedWriter bf = new BufferedWriter(osw);
			for (Integer key : outLiers) {
				bf.append(key.toString()).append("\n");
			}
			bf.close();
			osw.close();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	public static void main(String[] args) throws StorageException {
//		int window[] =  {1000,5000,10000,15000,20000}; 
//		for (int i = 0; i < window.length; i++) {
//			detectOutliers(fileName, 0.1, window[i], 6, 2, 30);
//		}
		if(Constant.flag==0) {
			detectOutliers(Constant.dataPath, Constant.relativeError,Constant.expire, Constant.K, Constant.L, Constant.alpha);
			List<Integer> outLiers = new ArrayList<>(outliers);
			Collections.sort(outLiers);
			System.out.println(outLiers.size());
			write(Constant.outputPath, outLiers);
			System.out.println("Over!");
		}else {
			findMIPS(Constant.topKcoll, Constant.relativeError, Constant.expire, Constant.K, Constant.L, Constant.topK);
			List<Integer> topk = new ArrayList<>(topK);
			Collections.sort(topk);
			System.out.println(topk.size());
			write(Constant.topKPath, topk);
			System.out.println("Over!");
		}
	}
}
