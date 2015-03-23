package test.singleThread;

import java.io.IOException;
import java.util.Comparator;

import tree.hb.HBGetTest;
import tree.hb.HBTree;
import tree.hb.HBPutTest;
import tree.hb.HBTreeOptimize;

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
	public void test(int chunkSize,String filePath,boolean optimize,int minLayerNum) throws IOException{
		if(optimize){
			System.out.println("HBTreeSingleThreadTest - Optimize:");
			HBTreeOptimize hbtreeop = new HBTreeOptimize(c,chunkSize,minLayerNum);
			HBPutTest pt = new HBPutTest(hbtreeop,filePath);
			pt.doPut();
			
			HBGetTest pg = new HBGetTest(hbtreeop,filePath);
			pg.doGet();	
		}else{
			System.out.println("HBTreeSingleThreadTest - Ordinary:");
			HBTree hbtree= new HBTree(c,chunkSize);
			HBPutTest pt = new HBPutTest(hbtree,filePath);
			pt.doPut();
			
			HBGetTest pg = new HBGetTest(hbtree,filePath);
			pg.doGet();	
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException{
		HBTreeSingleThreadTest hbt = new HBTreeSingleThreadTest();
		int chunkSize = 8;
		boolean optimize = true;
		int minLayerNum = 16;
//		String filePath = "D:/TestData/t2/keylen=16/1000w.txt";
		String filePath = "/TestData/t2/keylen=16 columnNum=1/500w/";
		
		hbt.test(chunkSize,filePath,optimize,minLayerNum);
	}
}
