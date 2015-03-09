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
			String value = s.split(" ")[1];
			System.out.println("key = "+key+" , keylen = "+key.length()+" , value = "+value);
			this.hbtree.add(key, new Value(value));
			totalNum++;
		}
		System.out.println("Total number: "+totalNum);
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
		HBTree hbtree= new HBTree(c);
		GetTest pg = new GetTest(hbtree,"d:/a.txt");
		pg.doGet();
	}

}
