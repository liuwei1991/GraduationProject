package tree.hb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;

import common.Value;

public class GetTest{
	private HBTree hbtree;
	private String inPutFilePath ;
	
	public GetTest(HBTree hbtree,String inPutFilePath){
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
		while(true){
			String s = br.readLine();
			if(s==null){
				break;
			}
			String key = s.split(" ")[0];
			String value1 = s.split(" ")[1];
			Value v = this.hbtree.get(key);
			String value2 = v==null?"null":v.getValue();
			System.out.println("key = "+key+" , keylen = "+key.length()+" , value1 = "+value1+" , value2 = "+value2);
			totalNum++;
		}
		System.out.println("Total number: "+totalNum);
	}
	
}
