package ac.iie.nnts.LSH;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import ac.iie.nnts.Constant.Constant;
import ac.iie.nnts.DW.DeterministicWave;
import ac.iie.nnts.DW.Element;
import ac.iie.nnts.DW.OverElement;
import io.hops.exception.StorageException;

/**
 * @author zhihui
 * Work out all hash values and mean count;
 */
public class LSH_topK {
	private int K;
	private int L;
	private double relativeError;
	private int expire;
    public DeterministicWave bucket[][];
    
	public LSH_topK(int K, int L,double relativeError,int expire) {
		this.K = K;
		this.L = L;
		this.relativeError = relativeError;
		this.expire = expire;
		bucket = new DeterministicWave[L][1<<K];
	}
	
	public void add(int[][] hashes, int time,int key) throws StorageException {
		int index = 0;
		for (int i = 0; i < L; i++) {
			index = 0;
			for (int j = 0; j < K; j++) {
				index = index << 1;
				index = index + hashes[i][j];//得到K位哈希值
			}
			if(bucket[i][index]==null) {
				bucket[i][index]=new DeterministicWave();
				//增加的 PGPGPGPG
				bucket[i][index].setParakey(100+i*10+index);
				bucket[i][index].setParaTable("para");
				bucket[i][index].Initialized(relativeError,expire);
			}
			bucket[i][index].input(key,1, time);		
		}
	}
	
	public void Query(int[][] hashes,int range) throws StorageException {
		int index;
		Map<Integer, ArrayList<Integer>>[] sim = new TreeMap[L];
		
		int[] base_lable = new int[L];
		for (int i = 0; i < L; i++) {//获得base的位置
			index = 0;
			for (int j = 0; j < K; j++) {
				index = index << 1;
				index = index + hashes[i][j];//得到K位哈希值
			}
			base_lable[i]=index;
		}
		for (int i = 0; i < L; i++) {//获得每个DW与base相同的位数
			sim[i] = new TreeMap<Integer, ArrayList<Integer>>(
					//每一行按照相似度排序，从大到小
	                new Comparator<Integer>() {
	                    public int compare(Integer obj1, Integer obj2) {
	                        // 降序排序
	                        return obj2.compareTo(obj1);
	                    }
	                });
			for(int j=0;j<1<<K;j++) {
				if(bucket[i][j]!=null) {
					int count = Integer.bitCount(j^base_lable[i]);
					if(!sim[i].containsKey(Integer.bitCount(j^base_lable[i])))
						sim[i].put(count,new ArrayList<>());//相同位数作为key,下标作为value
					sim[i].get(count).add(j);
				}
			}
		}
		//找topK
		int num = 0;
		while(num<Constant.topK) {
			for(int i=K;i>0;i--) {//从相似度最高的开始
				for(int j=0;j<L;j++) {//每一个行的并集
					if(sim[j].containsKey(i)) {//如果有这个相似度的
						
						for (int arr : sim[j].get(i)) {//加入
							for (OverElement e : bucket[j][arr].getOverflow()) {
								if(e.getTimestamp()>Constant.curTime-range) {
									LSH_Outlier.topK.add(e.getKey());
									num++;
									if(num>Constant.topK)break;
								}
							}
							for (Element e : bucket[j][arr].getAllInWindow()) {
								if(e.getTimestamp()>Constant.curTime-range) {
									LSH_Outlier.topK.add(e.getKey());
									num++;
									if(num>Constant.topK)break;
								}
							}
						}
						
						
					}
				}
			}
		}
	}
}
