package Test;

import java.io.IOException;
import java.util.Comparator;

import tree.bplus.BPGetTest;
import tree.bplus.BPlus;
import tree.bplus.BPPutTest;

public class BPlusTest {
	Comparator<String> c = new Comparator<String>() {
		@Override
		public int compare(String o1, String o2) {
			if (o1.compareTo(o2) < 0) {
				return -1;
			} else if (o1.compareTo(o2) == 0) {
				return 0;
			}
			return 1;
		}
	};

	public void test(String filePath) throws IOException{
		BPlus bskl = new BPlus(c);

		BPPutTest pt = new BPPutTest(bskl, filePath);
		pt.doPut();

		BPGetTest bgt = new BPGetTest(bskl, filePath);
		bgt.doGet();
	}
	
	public static void main(String[] args) throws IOException {
		BPlusTest bpt = new BPlusTest();
		String filePath = "D:/TestData/t2/keylen=16/1000w.txt";
		
		bpt.test(filePath);
	}
}
