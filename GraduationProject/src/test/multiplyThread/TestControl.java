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

	
	public void bPlusMultiGetTest() throws Exception{
		Date d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/bPlus-Get-Result-"
				+ d.getYear() + "-" + "-" + d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + ":" + d.getMinutes() +":"+d.getSeconds()+ ".txt";
		
		int keylen[] = { 8, 16, 24, 32 };
		int threadNum[] = { 8,16,24, 32 };
		int columnNum = 4;
		
		for (int i = 1; i <= 5; i++) {
			for (int len : keylen) {
				for (int tn : threadNum) {
					System.gc();
					Thread.sleep(5*1000);
					String inputFilePath = "/ares/TestData/t2/thread=" + tn
							+ "/keylen=" + len + " columnNum=" + columnNum
							+ "/" + 1000 * i + "w/";
					int rowNum = i * 1000 * 10000;
					BPlus bp = new BPlus(c);
					
					//Input data first.
					this.bPlusSinglePutTest(bp,rowNum, len, tn, columnNum,
							inputFilePath);
					
					//Call gc, record the memory used.
					System.gc();
					InputStreamReader isReader = new InputStreamReader(
							System.in);
					System.out.println("Record the memory used and the data info,then input a string: ");
					String str = new BufferedReader(isReader).readLine();
					System.out.println("Input String: " + str);
					//
					this.bPlusSingleGetTest(bp, tn, rowNum, columnNum, inputFilePath);
				}
			}
		}
	}
	
	private void bPlusSingleGetTest(BPlus bp,int threadNum,int rowNum,int columnNum,String inputFilePath) throws Exception{
		BPlusMultiThreadGetTest btg = new BPlusMultiThreadGetTest(bp,inputFilePath,threadNum);
		for (int j = 0; j < threadNum; j++) {
			Thread t = new Thread(btg);
			t.setName(String.valueOf(j));
			t.start();
		}
		
		while (BPlusMultiThreadGetTest.totalNum == 0) {
			Thread.sleep(200);
		}
		BPlusMultiThreadGetTest.time = System.currentTimeMillis();
		BPlusMultiThreadGetTest.startTime = BPlusMultiThreadGetTest.time;
		BPlusMultiThreadGetTest.targetNum = rowNum * columnNum;
		BPlusMultiThreadGetTest.totalNum = 0;
		BPlusMultiThreadGetTest.lastNum = 0;

		BPlusMultiThreadGetTest.output = new Thread(new BPlusMetrics());
		BPlusMultiThreadGetTest.output.setPriority(Thread.MAX_PRIORITY);
		BPlusMultiThreadGetTest.output.start();
		BPlusMultiThreadGetTest.output.join();
	}
	
	
	private void bPlusSinglePutTest(BPlus bp,int rowNum, int keyLen, int threadNum,
			int columnNum, String inputFilePath) throws Exception {
		
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

	public void bPlusMultiPutTest() throws Exception {

		Date d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/bPlus-put-Result-"
				+ d.getYear() + "-" + "-" + d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + ":" + d.getMinutes() +":"+d.getSeconds()+ ".txt";
		
		int keylen[] = { 8, 16, 24, 32 };
		int threadNum[] = { 8,16,24, 32 };
		int columnNum = 4;
		
		for (int i = 1; i <= 5; i++) {
			for (int len : keylen) {
				for (int tn : threadNum) {
					System.gc();
					Thread.sleep(5*1000);
					
//					InputStreamReader isReader = new InputStreamReader(
//							System.in);
//					System.out.println("Please input a String: ");
//					String str = new BufferedReader(isReader).readLine();
//					System.out.println("Input String: " + str);

					String inputFilePath = "/ares/TestData/t2/thread=" + tn
							+ "/keylen=" + len + " columnNum=" + columnNum
							+ "/" + 1000 * i + "w/";
					int rowNum = i * 1000 * 10000;
					BPlus bp = new BPlus(c);
					this.bPlusSinglePutTest(bp,rowNum, len, tn, columnNum,
							inputFilePath);
				}
			}
		}
	}
	
	
	public void hbTreeMultiGetTest() throws Exception {
		Date d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/hbTree-get-Result-"
				+ d.getYear() + "-" + "-" + d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + ":" + d.getMinutes() +":"+d.getSeconds()+ ".txt";
		
		int keylen[] = { 8, 16, 24, 32 };
		int threadNum[] = { 8,16,24,32};
		int chunkSize[] = { 32,28,24,20,16,12,8,4};
		boolean optimize = false;
		int minLayerNum = 16;
		int columnNum = 4;

		for (int tn : threadNum) {
			for (int cs : chunkSize) {
				for (int len : keylen) {
					for (int i = 1; i <= 5; i++) {
						if(cs>len) break;
						System.gc();
						Thread.sleep(5*1000);
						String inputFilePath = "/ares/TestData/t2/thread=" + tn
								+ "/keylen=" + len + " columnNum=" + columnNum
								+ "/" + 1000 * i + "w/";
						int rowNum = i * 1000 * 10000;
						HBTreeMultiThreadPutTest hbtp = null;
						if (optimize) {
							HBTreeOptimize hbtreeop = new HBTreeOptimize(c, cs,
									minLayerNum);
							hbtp = new HBTreeMultiThreadPutTest(hbtreeop, inputFilePath);
						} else {
							HBTree hbtree = new HBTree(c, cs);
							hbtp = new HBTreeMultiThreadPutTest(hbtree, inputFilePath);
						}
						
						//Input data first.
						this.hbTreeSinglePutTest(hbtp,rowNum, tn, columnNum, inputFilePath);
						
						//Call gc, record the memory used.
						System.gc();
						InputStreamReader isReader = new InputStreamReader(
								System.in);
						System.out.println("Record the memory used and the data info,then input a string: ");
						String str = new BufferedReader(isReader).readLine();
						System.out.println("Input String: " + str);
						//
						this.hbTreeSingleGetTest(hbtp, tn, rowNum, columnNum, inputFilePath);
					}
				}
			}
		}
	}
	
	private void hbTreeSingleGetTest(HBTreeMultiThreadPutTest hbtp,int threadNum,int rowNum,int columnNum,String inputFilePath) throws Exception{
		
		HBTreeMultiThreadGetTest hbgt = null;
		if(hbtp.isOptimize()){
			hbgt = new HBTreeMultiThreadGetTest( hbtp.getHbtreeop(), inputFilePath,threadNum);
		}else{
			hbgt = new HBTreeMultiThreadGetTest( hbtp.getHbtree(), inputFilePath,threadNum);
		}
		for (int i = 0; i < threadNum; i++) {
			Thread t = new Thread(hbgt);
			t.setName(String.valueOf(i));
			t.start();
		}
		while (HBTreeMultiThreadGetTest.totalNum == 0) {
			Thread.sleep(200);
		}

		HBTreeMultiThreadGetTest.lastNum = 0;
		HBTreeMultiThreadGetTest.totalNum = 0;
		HBTreeMultiThreadGetTest.targetNum = columnNum * rowNum;
		HBTreeMultiThreadGetTest.startTime = System.currentTimeMillis();
		HBTreeMultiThreadGetTest.time = HBTreeMultiThreadPutTest.startTime;

		HBTreeMultiThreadGetTest.output = new Thread(new HBTreeMetrics());
		HBTreeMultiThreadGetTest.output.setPriority(Thread.MAX_PRIORITY);
		HBTreeMultiThreadGetTest.output.start();
		HBTreeMultiThreadGetTest.output.join();
	}

	public void hbTreeMultiPutTest() throws Exception {
		Date d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/hbTree-put-Result-"
				+ d.getYear() + "-" + "-" + d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + ":" + d.getMinutes() +":"+d.getSeconds()+ ".txt";
		
		int threadNum[] = { 8,16,24, 32};
		int chunkSize[] = { 32,28,24,20,16,12,8,4};
		int keylen[] = { 8, 16, 24, 32 };
		boolean optimize = false;
		int minLayerNum = 16;
		int columnNum = 4;

		for (int tn : threadNum) {
			for (int cs : chunkSize) {
				for (int len : keylen) {
					for (int i = 1; i <= 5; i++) {
						if(cs>len) break;
						System.gc();
						Thread.sleep(5*1000);
						
//						InputStreamReader isReader = new InputStreamReader(
//								System.in);
//						System.out.println("Please input a String: ");
//						String str = new BufferedReader(isReader).readLine();
//						System.out.println("Input String: " + str);

						String inputFilePath = "/ares/TestData/t2/thread=" + tn
								+ "/keylen=" + len + " columnNum=" + columnNum
								+ "/" + 1000 * i + "w/";
						int rowNum = i * 1000 * 10000;
						HBTreeMultiThreadPutTest hbtp = null;
						if (optimize) {
							HBTreeOptimize hbtreeop = new HBTreeOptimize(c, cs,
									minLayerNum);
							hbtp = new HBTreeMultiThreadPutTest(hbtreeop, inputFilePath);
						} else {
							HBTree hbtree = new HBTree(c, cs);
							hbtp = new HBTreeMultiThreadPutTest(hbtree, inputFilePath);
						}
						this.hbTreeSinglePutTest(hbtp,rowNum, tn, columnNum, inputFilePath);
					}
				}
			}
		}
	}

	private void hbTreeSinglePutTest(HBTreeMultiThreadPutTest hbtp,int rowNum, int threadNum,
			int columnNum,String inputFilePath) throws InterruptedException {

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
		
		TestControl tc = new TestControl();
//		tc.bPlusMultiPutTest();
//		tc.hbTreeMultiPutTest();
		tc.bPlusMultiGetTest();
//		tc.hbTreeMultiGetTest();
	}

}
