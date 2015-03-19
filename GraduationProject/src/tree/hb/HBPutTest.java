package tree.hb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import common.Value;

public class HBPutTest {
	private HBTree hbtree;
	private HBTreeOptimize hbtreeop;
	private String inPutFilePath ;
	private boolean isOptimize = false;
	
	public HBPutTest(HBTree hbtree,String inPutFilePath){
		this.hbtree = hbtree;
		this.inPutFilePath = inPutFilePath;
	}
	
	public HBPutTest(HBTreeOptimize hbtreeop,String inPutFilePath){
		this.hbtreeop = hbtreeop;
		this.inPutFilePath = inPutFilePath;
		this.isOptimize = true;
	}
	
	public void doPut() throws IOException{
		if(this.isOptimize){
			this.doPutOptimize();
		}else{
			this.doPutOrdinary();
		}
	}
	
	public void doPutOrdinary() throws IOException{
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
			if(str==null){
				break;
			}
			String[] line = str.split(" ");
			Map<String,String> kvs = new HashMap<String,String>();
			for(int i=1;i<line.length;i++){
				kvs.put("column"+i, line[i]);
			}
//			System.out.println("key = "+key+" , keylen = "+key.length()+" , value = "+value);
			this.hbtree.add(line[0], kvs);
//			totalNum++;
//			if(totalNum%1000000==0){
//				System.out.println("Total num: "+totalNum);
//			}
		}
		Long e = System.currentTimeMillis();
		System.out.println("HBTree - PutTest - total number: "+totalNum+" , Total time: "+(e-s)/1000.0+"s. Speed:"+totalNum*1000.0/(e-s)+" /second.");
//		HBTree.printHBTree(this.hbtree.rootNode);
	}

	public void doPutOptimize() throws IOException{
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
			if(str==null){
				break;
			}
			String[] line = str.split(" ");
			Map<String,String> kvs = new HashMap<String,String>();
			for(int i=1;i<line.length;i++){
				kvs.put("column"+i, line[i]);
			}
//			System.out.println("key = "+key+" , keylen = "+key.length()+" , value = "+value);
			this.hbtreeop.add(line[0], kvs);
//			totalNum++;
//			if(totalNum%1000000==0){
//				System.out.println("Total num: "+totalNum);
//			}
		}
		Long e = System.currentTimeMillis();
		System.out.println("HBTree - PutTest - total number: "+totalNum+" , Total time: "+(e-s)/1000.0+"s. Speed:"+totalNum*1000.0/(e-s)+" /second.");
		HBTree.printHBTree(this.hbtreeop.rootNodeOpt);
	}
	public static void main(String[] args) throws IOException{
		Comparator<String> c = new Comparator<String>(){

			@Override
			public int compare(String o1, String o2) {
				if(o1.compareTo(o2)<0){
					return -1;
				}else if(o1.compareTo(o2)==0){
					return 0;
				}
				return 1;
			}
		};
		
		int chunkSize = 8;
		String filePath =  "d:/a.txt";
		
		HBTree hbtree= new HBTree(c,chunkSize);
		HBPutTest pt = new HBPutTest(hbtree,filePath);
		pt.doPut();
	}
	
}
