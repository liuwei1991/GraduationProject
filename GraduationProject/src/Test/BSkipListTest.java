package Test;

import java.io.IOException;
import java.util.Comparator;

import tree.bplus.BSkipList;
import tree.bplus.PutTest;

public class BSkipListTest {

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
		BSkipList bskl= new BSkipList(c);
		PutTest pt = new PutTest(bskl,"d:/a.txt");
		pt.doPut();
		
	}
}
