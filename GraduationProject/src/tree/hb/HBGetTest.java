package tree.hb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;

import test.CommonVariable;
import common.Value;

public class HBGetTest{
	private HBTree hbtree;
	private HBTreeOptimize hbtreeop;
	private boolean isOptimize = false;
	private String inPutFilePath ;
	
	public HBGetTest(HBTree hbtree,String inPutFilePath){
		this.hbtree = hbtree;
		this.inPutFilePath = inPutFilePath;
	}
	
	public HBGetTest(HBTreeOptimize hbtreeop,String inPutFilePath){
		this.hbtreeop = hbtreeop;
		this.inPutFilePath = inPutFilePath;
		this.isOptimize = true;
	}
	
	public void doGet() throws IOException{
		if(this.isOptimize){
			this.doGetOptimize();
		}else{
			this.doGetOrdinary();
		}
	}
	
	public void doGetOrdinary() throws IOException{
		File file = new File(this.inPutFilePath);
		if(!file.exists()){
			System.out.println("Input file is not found!");
			return;
		}
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);
		
		int totalNum = 0;
		Long s = System.currentTimeMillis();
		while(true){
			String str = br.readLine();
			if(str==null){
				break;
			}
			String line[] = str.split(" ");
			for(int j=1;j<line.length;j++){
				this.hbtree.get(line[0],CommonVariable.COLUMN+j);
				totalNum++;
			}
			if(totalNum%1000000==0){
				System.out.println("Total get num: "+totalNum);
			}
//			String key = str.split(" ")[0];
//			this.hbtree.get(key);
			
//			String value1 = s.split(" ")[1];
//			Value v = this.hbtree.get(key);
//			String value2 = v==null?"null":v.getValue();
//			System.out.println("key = "+key+" , keylen = "+key.length()+" , value1 = "+value1+" , value2 = "+value2);
		}
		Long e = System.currentTimeMillis();
//		System.out.println("Total number: "+totalNum);
		System.out.println("HBTree - GetTest - total number: "+totalNum+" , Total time: "+(e-s)/1000.0+"s. Speed:"+totalNum*1000.0/(e-s)+" /second.");
	}
	
	public void doGetOptimize() throws IOException{
		File file = new File(this.inPutFilePath);
		if(!file.exists()){
			System.out.println("Input file is not found!");
			return;
		}
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);
		
		int totalNum = 0;
		Long s = System.currentTimeMillis();
		while(true){
			String str = br.readLine();
			if(str==null){
				break;
			}
			String line[] = str.split(" ");
			for(int j=1;j<line.length;j++){
				this.hbtreeop.get(line[0],CommonVariable.COLUMN + j);
				totalNum++;
			}
			if(totalNum%1000000==0){
				System.out.println("Total get num: "+totalNum);
			}
//			String key = str.split(" ")[0];
//			this.hbtree.get(key);
			
//			String value1 = s.split(" ")[1];
//			Value v = this.hbtree.get(key);
//			String value2 = v==null?"null":v.getValue();
//			System.out.println("key = "+key+" , keylen = "+key.length()+" , value1 = "+value1+" , value2 = "+value2);
		}
		Long e = System.currentTimeMillis();
//		System.out.println("Total number: "+totalNum);
		System.out.println("HBTree - GetTest - total number: "+totalNum+" , Total time: "+(e-s)/1000.0+"s. Speed:"+totalNum*1000.0/(e-s)+" /second.");
	}
	
}
