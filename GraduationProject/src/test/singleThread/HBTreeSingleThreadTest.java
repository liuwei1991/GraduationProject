package test.singleThread;

import java.io.IOException;
import java.util.Comparator;

import tree.hb.HBGetTest;
import tree.hb.HBTree;
import tree.hb.HBPutTest;

public class HBTreeSingleThreadTest {
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
	public void test(int chunkSize,String filePath) throws IOException{
		HBTree hbtree= new HBTree(c,chunkSize);
		HBPutTest pt = new HBPutTest(hbtree,filePath);
		pt.doPut();
		
		HBGetTest pg = new HBGetTest(hbtree,filePath);
		pg.doGet();	
	}
	
	public static void main(String[] args) throws IOException, InterruptedException{
		HBTreeSingleThreadTest hbt = new HBTreeSingleThreadTest();
		int chunkSize = 8;
		String filePath = "D:/TestData/t2/keylen=16/1000w.txt";

		hbt.test(chunkSize,filePath);
	}
}
