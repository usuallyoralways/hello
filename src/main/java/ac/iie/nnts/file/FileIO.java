package ac.iie.nnts.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class FileIO {
	public static String inputPath = "E:/dataset/tao.txt";
	public static String outputPath = "E:/dataset/tao_key.txt";
	
	public static void main(String args[]) {
		try{
			//写入结果
			File file = new File(outputPath);//结果文件			
			FileOutputStream out = new FileOutputStream(file);
			OutputStreamWriter osw = new OutputStreamWriter(out,"UTF-8");
			BufferedWriter bf = new BufferedWriter(osw);
			//读入
			FileReader reader = new FileReader(inputPath);
			BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言
			String line;
			int i = 1;
			while ((line = br.readLine()) != null) {
			    bf.append(i+",").append(line).append("\n");//符号搞好
				i++;
			}
			reader.close();
			br.close();
			bf.close();
			osw.close();
			out.close();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	}

}
