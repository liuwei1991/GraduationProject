package Test;

import java.io.IOException;
import java.util.Comparator;

import tree.hb.HBGetTest;
import tree.hb.HBTree;
import tree.hb.HBPutTest;

public class HBTreeTest {
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
		String filePath = "d:/500w.txt";
//		int chunkSize[] ={2,4,6,8,16,24};
		int chunkSize[] ={24};
		
		for(int cs:chunkSize){
			HBTree hbtree= new HBTree(c,cs);
			System.out.println("chunkSize = "+cs);
			HBPutTest pt = new HBPutTest(hbtree,filePath);
			pt.doPut();
		
			HBGetTest pg = new HBGetTest(hbtree,filePath);
			pg.doGet();	
		}
		
	}
}
