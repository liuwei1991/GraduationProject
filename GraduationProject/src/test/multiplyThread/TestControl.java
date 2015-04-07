package test.multiplyThread;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.Date;

import tree.bplus.BPlus;
import tree.hb.HBTree;
import tree.hb.HBTreeOptimize;
import test.CommonVariable;
import test.multiplyThread.BPlusMultiThreadPutTest.BPlusMetrics;
import test.multiplyThread.HBTreeMultiThreadPutTest.HBTreeMetrics;

public class TestControl {

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

	public void BPlusSinglePutTest(int rowNum, int keyLen, int threadNum,
			int columnNum, String inputFilePath) throws Exception {
		BPlus bp = new BPlus(c);
		BPlusMultiThreadPutTest btp = new BPlusMultiThreadPutTest(bp,
				inputFilePath);

		for (int j = 0; j < threadNum; j++) {
			Thread t = new Thread(btp);
			t.setName(String.valueOf(j));
			t.start();
		}
		while (BPlusMultiThreadPutTest.totalNum == 0) {
			Thread.sleep(200);
		}
		BPlusMultiThreadPutTest.time = System.currentTimeMillis();
		BPlusMultiThreadPutTest.startTime = BPlusMultiThreadPutTest.time;
		BPlusMultiThreadPutTest.targetNum = rowNum * columnNum;
		BPlusMultiThreadPutTest.totalNum = 0;
		BPlusMultiThreadPutTest.lastNum = 0;

		BPlusMultiThreadPutTest.output = new Thread(new BPlusMetrics());
		BPlusMultiThreadPutTest.output.setPriority(Thread.MAX_PRIORITY);
		BPlusMultiThreadPutTest.output.start();
		BPlusMultiThreadPutTest.output.join();
	}

	public void BPlusMultiThreadPutTest() throws Exception {

		int keylen[] = { 8, 16, 24, 32 };
		int threadNum[] = { 8,16,24, 32 };
		int columnNum = 4;

		for (int i = 1; i <= 5; i++) {
			for (int len : keylen) {
				for (int tn : threadNum) {
					System.gc();
					Thread.sleep(10*1000);
//					InputStreamReader isReader = new InputStreamReader(
//							System.in);
//					System.out.println("Please input a String: ");
//					String str = new BufferedReader(isReader).readLine();
//					System.out.println("Input String: " + str);

					String inputFilePath = "/ares/TestData/t2/thread=" + tn
							+ "/keylen=" + len + " columnNum=" + columnNum
							+ "/" + 1000 * i + "w/";
					int rowNum = i * 1000 * 10000;
					this.BPlusSinglePutTest(rowNum, len, tn, columnNum,
							inputFilePath);
				}
			}
		}
	}

	public void HBTreeMultiThreadPutTest() throws Exception {
		int keylen[] = { 8, 16, 24, 32 };
		int threadNum[] = { 8,16,24, 32 };
		int chunkSize[] = { 4, 8, 12, 16, 20, 24, 28, 32};
		boolean optimize = false;
		int minLayerNum = 16;
		int columnNum = 4;

		for (int i = 1; i <= 5; i++) {
			for (int len : keylen) {
				for (int tn : threadNum) {
					for (int cs : chunkSize) {
						if(cs>len) break;
						System.gc();
						Thread.sleep(10*1000);
						
//						InputStreamReader isReader = new InputStreamReader(
//								System.in);
//						System.out.println("Please input a String: ");
//						String str = new BufferedReader(isReader).readLine();
//						System.out.println("Input String: " + str);

						String inputFilePath = "/ares/TestData/t2/thread=" + tn
								+ "/keylen=" + len + " columnNum=" + columnNum
								+ "/" + 1000 * i + "w/";
						int rowNum = i * 1000 * 10000;
						this.HBTreeSinglePutTest(rowNum, len, tn, columnNum,
								cs, optimize, minLayerNum, inputFilePath);
					}
				}
			}
		}

	}

	public void HBTreeSinglePutTest(int rowNum, int keyLen, int threadNum,
			int columnNum, int chunkSize, boolean optimize, int minLayerNum,
			String inputFilePath) throws InterruptedException {

		HBTreeMultiThreadPutTest hbtp = null;
		if (optimize) {
			HBTreeOptimize hbtreeop = new HBTreeOptimize(c, chunkSize,
					minLayerNum);
			hbtp = new HBTreeMultiThreadPutTest(hbtreeop, inputFilePath);
		} else {
			HBTree hbtree = new HBTree(c, chunkSize);
			hbtp = new HBTreeMultiThreadPutTest(hbtree, inputFilePath);
		}
		for (int i = 0; i < threadNum; i++) {
			Thread t = new Thread(hbtp);
			t.setName(String.valueOf(i));
			t.start();
		}
		while (HBTreeMultiThreadPutTest.totalNum == 0) {
			Thread.sleep(200);
		}

		HBTreeMultiThreadPutTest.lastNum = 0;
		HBTreeMultiThreadPutTest.totalNum = 0;
		HBTreeMultiThreadPutTest.targetNum = columnNum * rowNum;
		HBTreeMultiThreadPutTest.startTime = System.currentTimeMillis();
		HBTreeMultiThreadPutTest.time = HBTreeMultiThreadPutTest.startTime;

		HBTreeMultiThreadPutTest.output = new Thread(new HBTreeMetrics());
		HBTreeMultiThreadPutTest.output.setPriority(Thread.MAX_PRIORITY);
		HBTreeMultiThreadPutTest.output.start();
		HBTreeMultiThreadPutTest.output.join();
	}

	public static void main(String[] args) throws Exception {
		TestControl tc = new TestControl();

		Date d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/bPlusResult-"
				+ d.getYear() + "-" + "-" + d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + "-" + d.getMinutes() +"-"+d.getSeconds()+ ".txt";
		tc.BPlusMultiThreadPutTest();
		
		
		d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/hbTreeResult-"
				+ d.getYear() + "-" + "-" + d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + "-" + d.getMinutes() +"-"+d.getSeconds()+ ".txt";
		tc.HBTreeMultiThreadPutTest();
	}

}
