package tree.hb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;

import common.Value;

public class HBGetTest{
	private HBTree hbtree;
	private String inPutFilePath ;
	
	public HBGetTest(HBTree hbtree,String inPutFilePath){
		this.hbtree = hbtree;
		this.inPutFilePath = inPutFilePath;
	}
	
	public void doGet() throws IOException{
		File file = new File(this.inPutFilePath);
		if(!file.exists()){
			System.out.println("Input file is not found!");
			return;
		}
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		
		int totalNum = 0;
		Long s = System.currentTimeMillis();
		while(true){
			String str = br.readLine();
			String column = "q1";
			if(str==null){
				break;
			}
			this.hbtree.get(str.split(" ")[0],column);
			
//			String key = str.split(" ")[0];
//			this.hbtree.get(key);
			
//			String value1 = s.split(" ")[1];
//			Value v = this.hbtree.get(key);
//			String value2 = v==null?"null":v.getValue();
//			System.out.println("key = "+key+" , keylen = "+key.length()+" , value1 = "+value1+" , value2 = "+value2);
//			totalNum++;
		}
		Long e = System.currentTimeMillis();
//		System.out.println("Total number: "+totalNum);
		System.out.println("HBTree - GetTest - total number: "+totalNum+" , Total time: "+(e-s)/1000.0+"s. Speed:"+totalNum*1000.0/(e-s)+" /second.");
	}
	
}
