package tree.bplus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;

import test.CommonVariable;
import common.Value;

public class BPGetTest {

	private BPlus bskl;
	private String inPutFilePath;
	
	public BPGetTest(BPlus bskl,String inPutFilePath){
		this.bskl = bskl;
		this.inPutFilePath = inPutFilePath;
	}
	
	public void doGet() throws IOException{
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
			String[] line = str.split(" ");
//			Map<String,String> kvs = new HashMap<String,String>();
			for(int i=1;i<line.length;i++){
				this.bskl.get(line[0],CommonVariable.COLUMN+i);
				totalNum++;
			}
			if(totalNum%1000000==0){
				System.out.println("Total get num: "+totalNum);
			}
//			String value = str.split(" ")[1];
//			System.out.println("key = "+key+" , keylen = "+key.length()+" , value = "+value);
//			totalNum++;
		}
		br.close();
		fr.close();
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
		BPGetTest gt = new BPGetTest(bskl,"d:/a.txt");
		gt.doGet();
	}
}
