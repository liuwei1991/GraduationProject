package tree.bplus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;

public class PutTest {
	private BSkipListTest bskl;
	private String inPutFilePath;
	
	public PutTest(BSkipListTest bskl,String inPutFilePath){
		this.bskl = bskl;
		this.inPutFilePath = inPutFilePath;
	}
	
	public void doInput() throws IOException{
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
			this.bskl.add(key, new Value(value));
			totalNum++;
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
				}
				return 1;
			}
		};
		BSkipListTest bskl= new BSkipListTest(c);
		PutTest pt = new PutTest(bskl,"d:/a.txt");
		pt.doInput();
	}
	

}
