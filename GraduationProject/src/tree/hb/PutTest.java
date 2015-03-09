package tree.hb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;

import common.Value;

public class PutTest {
	private HBTree hbtree;
	private String inPutFilePath ;
	
	public PutTest(HBTree hbtree,String inPutFilePath){
		this.hbtree = hbtree;
		this.inPutFilePath = inPutFilePath;
	}
	
	public void doPut() throws IOException{
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
			String key = str.split(" ")[0];
			String value = str.split(" ")[1];
//			System.out.println("key = "+key+" , keylen = "+key.length()+" , value = "+value);
			this.hbtree.add(key, new Value(value));
			totalNum++;
		}
		Long e = System.currentTimeMillis();
		System.out.println("HBTree - PutTest - total number: "+totalNum+" , Total time: "+(e-s)/1000.0+"s. Speed:"+totalNum*1000.0/(e-s)+" /second.");
//		HBTree.printHBTree(this.hbtree.rootNode);
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
		HBTree hbtree= new HBTree(c);
		PutTest pt = new PutTest(hbtree,"d:/a.txt");
		pt.doPut();
	}
	
}
