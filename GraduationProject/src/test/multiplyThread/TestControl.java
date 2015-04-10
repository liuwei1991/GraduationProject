package test.multiplyThread;

import java.io.FileWriter;
import java.util.Comparator;
import java.util.Date;

import tree.bplus.BPlus;
import tree.hb.HBTree;
import tree.hb.HBTreeOptimize;
import test.CommonVariable;
import test.multiplyThread.HBTreeMultiThreadPutTest.HBTreePutMetrics;
import test.multiplyThread.HBTreeMultiThreadGetTest.HBTreeGetMetrics;
import test.multiplyThread.BPlusMultiThreadPutTest.BPlusPutMetrics;
import test.multiplyThread.BPlusMultiThreadGetTest.BPlusGetMetrics;

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

		BPlusMultiThreadGetTest.output = new Thread(new BPlusGetMetrics());
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

		BPlusMultiThreadPutTest.output = new Thread(new BPlusPutMetrics());
		BPlusMultiThreadPutTest.output.setPriority(Thread.MAX_PRIORITY);
		BPlusMultiThreadPutTest.output.start();
		BPlusMultiThreadPutTest.output.join();
	}

	public void bPlusMultiPutTest() throws Exception {

		Date d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/bPlus-put-Result-"
				+ d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + ":" + d.getMinutes() +":"+d.getSeconds()+ ".txt";
		
		int keylen[] = { 8, 16, 24, 32 };
		int threadNum[] = { 8,16};
		int columnNum = 4;
		
		for (int tn : threadNum) {
			for (int i = 1; i <= 5; i++) {
				for (int len : keylen) {
					System.gc();
					Thread.sleep(3*1000);
					
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

		HBTreeMultiThreadGetTest.time = System.currentTimeMillis();
		HBTreeMultiThreadGetTest.startTime = HBTreeMultiThreadGetTest.time;
		HBTreeMultiThreadGetTest.targetNum = columnNum * rowNum;
		HBTreeMultiThreadGetTest.lastNum = 0;
		HBTreeMultiThreadGetTest.totalNum = 0;

		HBTreeMultiThreadGetTest.output = new Thread(new HBTreeGetMetrics());
		HBTreeMultiThreadGetTest.output.setPriority(Thread.MAX_PRIORITY);
		HBTreeMultiThreadGetTest.output.start();
		HBTreeMultiThreadGetTest.output.join();
	}

	public void hbTreeMultiPutTest() throws Exception {
		Date d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/hbTree-put-Result-"
				+ d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + ":" + d.getMinutes() +":"+d.getSeconds()+ ".txt";
		
		int threadNum[] = { 8,16};
		int chunkSize[] = { 32,28,24,20,16,12,8};
		int keylen[] = { 8, 16, 24, 32 };
		boolean optimize[] = {false,true};
		int minLayerNum[] = {64,128,256,512,1024};
		int columnNum = 4;
		for(boolean opt:optimize){
			for (int tn : threadNum) {
				for (int i = 4; i <= 5; i++) {
					for (int cs : chunkSize) {
						for (int len : keylen) {
							if(cs>len) continue;
							System.gc();
							Thread.sleep(3*1000);
							
							String inputFilePath = "/ares/TestData/t2/thread=" + tn
									+ "/keylen=" + len + " columnNum=" + columnNum
									+ "/" + 1000 * i + "w/";
							int rowNum = i * 1000 * 10000;
							
							HBTreeMultiThreadPutTest hbtp = null;
							if (opt) {
								for(int minLayer:minLayerNum){
									HBTreeOptimize hbtreeop = new HBTreeOptimize(c, cs,	minLayer);
									hbtp = new HBTreeMultiThreadPutTest(hbtreeop, inputFilePath);
									this.hbTreeSinglePutTest(hbtp,rowNum, tn, columnNum, inputFilePath);
								}
							} else {
								HBTree hbtree = new HBTree(c, cs);
								hbtp = new HBTreeMultiThreadPutTest(hbtree, inputFilePath);
								this.hbTreeSinglePutTest(hbtp,rowNum, tn, columnNum, inputFilePath);
							}
							
						}
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

		HBTreeMultiThreadPutTest.output = new Thread(new HBTreePutMetrics());
		HBTreeMultiThreadPutTest.output.setPriority(Thread.MAX_PRIORITY);
		HBTreeMultiThreadPutTest.output.start();
		HBTreeMultiThreadPutTest.output.join();
	}

	public void bPlusMultiTest() throws Exception{
		Date d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/bPlus-Get-Result-"
				+ d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + ":" + d.getMinutes() +":"+d.getSeconds()+ ".txt";
		
		int keylen[] = { 8, 16, 24, 32 };
		int threadNum[] = { 8,16 };
		int columnNum = 4;
		
//		int keylen[] = { 8 };
//		int threadNum[] = { 8 };
//		int columnNum = 4;
		
		for (int tn : threadNum) {
			for (int i = 1; i <= 5; i++) {
				for (int len : keylen) {
					System.gc();
					Thread.sleep(3*1000);
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
					
					Runtime run = Runtime.getRuntime();
					long usedMemory = run.totalMemory()-run.freeMemory();
					System.out.println("Used Memory: "+usedMemory);
					FileWriter resultWriter = new FileWriter(CommonVariable.RESULT_FILE_PATH,true);
					resultWriter.write("\r\nUsed Memory: "+usedMemory);
					
					long startTime = System.currentTimeMillis();
					bp.testScan();
					long timeUsed = System.currentTimeMillis() - startTime;
					System.out.println("Scan time used: " + timeUsed);
					resultWriter.write("\r\nScan time used: " + timeUsed);
					resultWriter.close();
					
					System.gc();
					this.bPlusSingleGetTest(bp, tn, rowNum, columnNum, inputFilePath);
				}
			}
		}
	}
	
	public void hbTreeMultiTest() throws Exception {
		Date d = new Date(System.currentTimeMillis());
		CommonVariable.RESULT_FILE_PATH = "/ares/result/hbTree-get-Result-"
				+ d.getMonth() + "-" + d.getDay()
				+ "-" + d.getHours() + ":" + d.getMinutes() +":"+d.getSeconds()+ ".txt";
		
		int threadNum[] = { 8,16};
		int chunkSize[] = { 32,28,24,20,16,12,8,4};
		int keylen[] = { 8,16,24,32 };
		boolean optimize[] = {false,true};
		int minLayerNum[] = {64,128,256,512,1024};
		int columnNum = 4;
		
//		int threadNum[] = { 8};
//		int chunkSize[] = {8,4,2};
//		int keylen[] = { 8 };
//		boolean optimize[] = {true};
//		int minLayerNum[] = {64,128,256,512,1024};
//		int columnNum = 4;
		
		for(boolean opt:optimize){
			for (int tn : threadNum) {
				for (int i = 1; i <= 5; i++) {
					for (int cs : chunkSize) {
						for (int len : keylen) {
							if(cs>len) continue;
							Thread.sleep(3*1000);
							String inputFilePath = "/ares/TestData/t2/thread=" + tn
									+ "/keylen=" + len + " columnNum=" + columnNum
									+ "/" + 1000 * i + "w/";
							int rowNum = i * 1000 * 10000;
							HBTreeMultiThreadPutTest hbtp = null;
							if (opt) {
								for(int minLayer:minLayerNum){
									System.gc();
									HBTreeOptimize hbtreeop = new HBTreeOptimize(c, cs, minLayer);
									hbtp = new HBTreeMultiThreadPutTest(hbtreeop, inputFilePath);
									
									//Input data first.
									this.hbTreeSinglePutTest(hbtp,rowNum, tn, columnNum, inputFilePath);
									
									//Call gc, record the memory used.
									System.gc();
									
									Runtime run = Runtime.getRuntime();
									long usedMemory = run.totalMemory()-run.freeMemory();
									System.out.println("Used Memory: "+usedMemory+",MinLayerNum: "+minLayer);
									FileWriter resultWriter = new FileWriter(CommonVariable.RESULT_FILE_PATH,true);
									resultWriter.write("\r\nUsed Memory: "+usedMemory+",MinLayerNum: "+minLayer);
									
									long startTime = System.currentTimeMillis();
									HBTree.printHBTree(hbtp.getHbtreeop().rootNodeOpt);
									long timeUsed = System.currentTimeMillis() - startTime;
									System.out.println("Optimized scan time used: " + timeUsed);
									resultWriter.write("\r\nOptimzed scan time used: " + timeUsed);
									resultWriter.close();
									
									System.gc();
									this.hbTreeSingleGetTest(hbtp, tn, rowNum, columnNum, inputFilePath);
								}
								
							} else {
								System.gc();
								HBTree hbtree = new HBTree(c, cs);
								hbtp = new HBTreeMultiThreadPutTest(hbtree, inputFilePath);
								//Input data first.
								this.hbTreeSinglePutTest(hbtp,rowNum, tn, columnNum, inputFilePath);
								
								//Call gc, record the memory used.
								System.gc();
								
								Runtime run = Runtime.getRuntime();
								long usedMemory = run.totalMemory()-run.freeMemory();
								System.out.println("Used Memory: "+usedMemory);
								FileWriter resultWriter = new FileWriter(CommonVariable.RESULT_FILE_PATH,true);
								resultWriter.write("\r\nUsed Memory: "+usedMemory);

								long startTime = System.currentTimeMillis();
								HBTree.printHBTree(hbtp.getHbtree().rootNode);
								long timeUsed = System.currentTimeMillis() - startTime;
								System.out.println("Scan time used: " + timeUsed);
								resultWriter.write("\r\nScan time used: " + timeUsed);
								resultWriter.close();
								
								System.gc();
								this.hbTreeSingleGetTest(hbtp, tn, rowNum, columnNum, inputFilePath);
								
//								InputStreamReader isReader = new InputStreamReader(
//										System.in);
//								System.out.println("Record the memory used and the data info,then input a string: ");
//								String str = new BufferedReader(isReader).readLine();
//								System.out.println("Input String: " + str);
								//
							}
							
						}
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		TestControl tc = new TestControl();
//		tc.bPlusMultiTest();
		tc.hbTreeMultiTest();
	}

}
