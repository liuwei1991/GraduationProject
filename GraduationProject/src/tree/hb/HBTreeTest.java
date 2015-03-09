package tree.hb;

import java.io.IOException;
import java.util.Comparator;

public class HBTreeTest {
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
		PutTest pt = new PutTest(hbtree,"d:/a.txt");
		pt.doInput();
	
		GetTest pg = new GetTest(hbtree,"d:/a.txt");
		pg.doGet();
	}
}
