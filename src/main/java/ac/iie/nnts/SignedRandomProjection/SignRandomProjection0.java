package ac.iie.nnts.SignedRandomProjection;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @author zhihui
 *
 */
public class SignRandomProjection0 {
	
	private int numOfHashes, samSize,K,L;
	private double[][] randBits;
	private int[][] indices;
	private int []  imp;
	
	public SignRandomProjection0(int dim, int K, int L) {
		this.K = K;
		this.L = L;
		this.numOfHashes = K*L;//一共又L行，每行需要K个哈希，因为每一位需要一次哈希
		if(dim>20)
			samSize = (int) Math.ceil(dim/3);
		else if(dim>5)
			samSize = 5;
		else if(dim>2)
			samSize =dim-1;
		else 
			samSize = dim;
		ArrayList<Integer> aList = new ArrayList<Integer>();
		for (int i = 0; i < dim; i++) {
			aList.add(i);
		}
		
		//workout all hash bits
		//randBits中保存加或者减的标志，这一维是加还是减
		randBits = new double[numOfHashes][samSize];
		//indices保存每次参与计算的维坐标，因为并不是所有维度都参与运算
		indices  = new int[numOfHashes][samSize];
		//imp是一个关于是否计算向量长度的标志（额外处理，平衡不同维度直差太大）
		imp = new int[numOfHashes];
		
		 for (int i = 0; i < numOfHashes; i++) {
			 imp[i] = (int) (Math.random()*samSize);
			 Collections.shuffle(aList);
			 for (int j = 0; j < samSize; j++) {
				indices[i][j] = aList.get(j);
				int curr = (int) (Math.random()*10);
				if(curr%2==0) randBits[i][j] = 1;
				else randBits[i][j] = -1;
			}
		}
	}
	
	public int[][] getHash(double[]  data) {
		
		int[][] hashes =  new int[L][K]; 
		for (int i = 0; i < L; i++) {			
			for (int j = 0; j < K; j++) {
				double s = 0;
				int index = i*K+j;
				
				double beta= 0;
				for (int q = 0; q < samSize; q++) {
					beta = beta + data[indices[index][q]]*data[indices[index][q]];
//					System.out.print(q+" ");///////////////////////
				}
				beta = Math.sqrt(beta);
//				System.out.println(beta);//////////////////////////
				for (int q = 0; q < samSize; q++) {
					double v = data[indices[index][q]];
					if(randBits[index][q]>0) {
						if(imp[index]==q) s=s+v*beta;
						else s=s+v;
//						System.out.print(s+" ");
//						s=s+v;
					}else {
						if(imp[index]==q) s=s-v*beta;
						else s=s-v;
//						System.out.print(s+" ");
//						s=s-v;
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
