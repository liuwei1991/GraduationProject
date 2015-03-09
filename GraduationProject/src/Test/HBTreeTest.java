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
		HBTree hbtree= new HBTree(c);
		HBPutTest pt = new HBPutTest(hbtree,"d:/a.txt");
		pt.doPut();
	
		HBGetTest pg = new HBGetTest(hbtree,"d:/a.txt");
		pg.doGet();
	}
}
