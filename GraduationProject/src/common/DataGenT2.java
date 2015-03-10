package common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/*
 * github: https://github.com/liuwei1991/GraduationProject.git
 */
public class DataGenT2 {
	int num = 0;
	int keyLen = 8;
	String filePath = "";
	
	public DataGenT2(int num,int keyLen,String filePath){
		this.num = num;
		this.keyLen = keyLen;
		this.filePath = filePath;
	}
	
	public void genData() throws IOException{
		File file = new File(this.filePath+".txt");
		
		if(file.exists()){
			System.out.println("File exists, newly created.");
		}
		file.createNewFile();
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);
		Random r = new Random();
		int totalNum = 0;
		
		for(int i=0;i<this.num;i++){
			String rowKey = "";
			String value = "";
			for(int j=0;j<keyLen;j++){
				rowKey += String.valueOf(r.nextInt(10));
			}
			value = String.valueOf(r.nextInt(10000));
			bw.write(rowKey+" "+value+"\r\n");
			totalNum++;
		}
		bw.flush();
		bw.close();
		fw.close();
		System.out.println("Number: "+totalNum);
	}
	
	public void genData(int fileNum) throws IOException{
		int numPerFile = this.num/num;
		int totalNum = 0;
		for(int curNum=0;curNum<fileNum;curNum++){
			File file = new File(this.filePath+"/"+curNum+".txt");
			
			if(file.exists()){
				System.out.println("File exists, newly created.");
			}
			file.createNewFile();
			FileWriter fw = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(fw);
			Random r = new Random();
			
			for(int i=0;i<numPerFile;i++){
				String rowKey = "";
				String value = "";
				for(int j=0;j<keyLen;j++){
					rowKey += String.valueOf(r.nextInt(10));
				}
				value = String.valueOf(r.nextInt(10000));
				bw.write(rowKey+" "+value+"\r\n");
				totalNum++;
			}
			bw.flush();
			bw.close();
			fw.close();
			System.out.println("File-"+curNum+" number: "+numPerFile);
		}
		System.out.println("Total number: "+totalNum);
	}
	
	public static void main(String[] args) throws IOException{
		int base = 10000;
		int keylen[] = {24,32};
		
		for(int len:keylen){
			for(int i=1;i<=10;i++){
				DataGenT2 dg = new DataGenT2(500*i*base,len,"D:/TestData/t2/keylen="+len+"/"+500*i+"w");
				dg.genData();
			}
		}
	}
}
