package common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/*
 * github: https://github.com/liuwei1991/GraduationProject.git
 */
public class DataGenT1 {
	int num = 0;
	int keyLen = 8;
	int columnNum = 4;
	String filePath = "";
	
	public DataGenT1(int num,int keyLen,int columnNum,String filePath){
		this.num = num;
		this.keyLen = keyLen;
		this.columnNum = columnNum;
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
		int base = (int) Math.pow(10.0, this.keyLen/4.0-1);
		Random r = new Random();
		int totalNum = 0;
		int n = (int) Math.pow(this.num/1.0, 4.0/this.keyLen)+1;
		for(int i=0;i<this.num;i++){
			String rowKey = "";
			for(int j=0;j<keyLen/4;j++){
				rowKey += String.valueOf(base+r.nextInt(n));
			}
			for(int j=0;j<this.columnNum;j++){
				rowKey+=" "+r.nextInt(10000);
			}
			bw.write(rowKey+"\r\n");
			totalNum++;
		}
		bw.flush();
		bw.close();
		fw.close();
		System.out.println("Number: "+totalNum);
	}
	
	public void genData(int fileNum) throws IOException{
		int numPerFile = this.num/num;
		int base = (int) Math.pow(10.0, this.keyLen/4.0-1);
		int n = (int) Math.pow(this.num/1.0, 4.0/this.keyLen)+1;

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
				for(int j=0;j<keyLen/4;j++){
					rowKey += String.valueOf(base+r.nextInt(n));
				}
				for(int j=0;j<this.columnNum;j++){
					rowKey+=" "+r.nextInt(10000);
				}
				bw.write(rowKey+"\r\n");
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
		int totalNum = 1500;
		int keylen = 16;
		String filePath = "D:/TestData/t1/"+totalNum+"w";
		for(int i=7;i<=10;i++){
			DataGenT1 dg = new DataGenT1(500*i*base,keylen,4,"D:/TestData/t1/"+500*i+"w");
			dg.genData();
		}
//		DataGen dg = new DataGen(totalNum*base,keylen,filePath);
//		dg.genData();
	}
}
