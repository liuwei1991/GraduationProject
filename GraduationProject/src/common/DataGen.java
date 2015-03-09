package common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/*
 * github: https://github.com/liuwei1991/GraduationProject.git
 */
public class DataGen {
	int num = 0;
	int keyLen = 8;
	String filePath = "";
	
	public DataGen(int num,int keyLen,String filePath){
		this.num = num;
		this.keyLen = keyLen;
		this.filePath = filePath;
	}
	
	public void genData() throws IOException{
		File file = new File(this.filePath);
		
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
			String value = "";
			for(int j=0;j<keyLen/4;j++){
				rowKey += String.valueOf(base+r.nextInt(n));
			}
			value = String.valueOf(base+r.nextInt(n));
			bw.write(rowKey+" "+value+"\r\n");
			totalNum++;
		}
		bw.flush();
		bw.close();
		fw.close();
		System.out.println("Number: "+totalNum);
	}
	
	public static void main(String[] args) throws IOException{
		int totalNum = 5000000;
		int keylen = 16;
		String filePath = "d:/a.txt";
		
		DataGen dg = new DataGen(totalNum,keylen,filePath);
		dg.genData();
	}
}
