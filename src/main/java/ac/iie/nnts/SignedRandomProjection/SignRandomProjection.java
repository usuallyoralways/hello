package ac.iie.nnts.SignedRandomProjection;

import java.util.ArrayList;
import java.util.Collections;

import ac.iie.nnts.Constant.Constant;

/**
 * @author zhihui
 *
 */
public class SignRandomProjection {
	
	private int numOfHashes, samSize,K,L,all;
	private byte[][] randBits;
	private int[][] indices;
	
	public SignRandomProjection(int dim, int K, int L) {
		this.K = K;
		this.L = L;
		if(dim>20)
			samSize = (int) Math.ceil(dim/3);
		else if(dim>5)
			samSize = 5;
		else if(dim>3)
			samSize =dim-1;
		else 
			samSize = dim;
		ArrayList<Integer> aList = new ArrayList<Integer>();
		for (int i = 0; i < dim; i++) {
			aList.add(i);
		}
		
		//workout all hash bits
			
		//randBits中保存加或者减的标志，这一维是加还是减
		this.numOfHashes = K*L;//一共又L行，每行需要K个哈希，因为每一位需要一次哈希
		randBits = new byte[numOfHashes][samSize];
		for(int p=0;p<L;p++) {
			ArrayList<byte[]> Bits=getBits(samSize);
			Collections.shuffle(Bits);
			for(int i=0;i<K;i++) {
				randBits[p*K+i]=Bits.get(i);
			}
		}
		
		//indices保存每次参与计算的维坐标，因为并不是所有维度都参与运算
		indices  = new int[numOfHashes][samSize];
		 for (int i = 0; i < numOfHashes; i++) {
			 //需要numOfHashes次哈希，也就是需要numOfHashes个w向量
			 Collections.shuffle(aList);
			 for (int j = 0; j < samSize; j++) {//每个w向量有samSize维
				indices[i][j] = aList.get(j);//这是采样的维度，也就是原始数据随机挑几个维度
			}
		}
	}
	
	public ArrayList<byte[]> getBits(int sampleSize){
	   ArrayList<byte[]> Bits = new ArrayList<>();	
		all = 1<<sampleSize;
		if(all<Constant.K) {
			K=all;
			Constant.K=all;
		}
		int[] idx ;
		ArrayList<Integer> all_t = new ArrayList<>();
		for(int i=0;i<all;i++)//all是最多的可能
			all_t.add(i);
		Collections.shuffle(all_t);
		idx=new int[K];
		for(int i=0;i<K;i++)//如果all<K,K=all
			idx[i]=i;
		
		for(int i=0;i<K;i++) {
			byte[] bs = new byte[sampleSize];
			int tmp=1;
			for(int j=0;j<sampleSize;j++) {
				if((idx[i]&tmp)==0) bs[j]=1;
				else bs[j]=-1;
				tmp=tmp<<1;
			}
			Bits.add(bs);
		}
		return Bits;
	}
	
	public int[][] getHash(double[]  data) {
		
		int[][] hashes =  new int[L][K]; 
		for (int i = 0; i < L; i++) {			
			for (int j = 0; j < K; j++) {
				double s = 0;
				int index = i*K+j;
				
				for (int q = 0; q < samSize; q++) {
					double v = data[indices[index][q]];
					if(randBits[index][q]>0) {
//						System.out.print(s+" ");
						s=s+v;
					}else {
//						System.out.print(s+" ");
						s=s-v;
					}
				}
//				System.out.println("===========================");
				hashes[i][j] = (s >= 0 ? 0 : 1);
//				System.out.print(hashes[i][j] );
			}
//			System.out.println("===========================");

		}
		return hashes;
	}
	

}
