package tree.bplus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import test.CommonVariable;
import common.Value;

public class BPPutTest {
	private BPlus bskl;
	private String inputFilePath;
	
	public BPPutTest(BPlus bskl,String inputFilePath){
		this.bskl = bskl;
		this.inputFilePath = inputFilePath;
	}
	
	public void doPut() throws IOException{
		File file = new File(this.inputFilePath);
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
//			Map<String,String> kvs = new HashMap<String,String>();
			for(int i=1;i<line.length;i++){
				this.bskl.add(line[0],CommonVariable.COLUMN+i, line[0]+","+line[i]);
				totalNum++;
			}
			if(totalNum%1000000==0){
				System.out.println("Total put num: "+totalNum);
			}
//			System.out.println("key = "+key+" , keylen = "+key.length()+" , value = "+value);
//			this.bskl.add(line[0], kvs);
		}
		Long e = System.currentTimeMillis();
		System.out.println("BSkipList - PutTest - total number: "+totalNum+" , Total time: "+(e-s)/1000.0+"s. Speed:"+totalNum*1000.0/(e-s)+" /second.");
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
		BPlus bskl= new BPlus(c);
		BPPutTest pt = new BPPutTest(bskl,"d:/a.txt");
		pt.doPut();
	}
}
