package test.multiplyThread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import test.CommonVariable;
import tree.bplus.BPlus;
import tree.hb.HBPutTest;
import tree.hb.HBTree;
import tree.hb.HBTreeOptimize;
import common.Value;

public class HBTreeMultiThreadPutTest implements Runnable {
	private HBTree hbtree;
	private HBTreeOptimize hbtreeop;
	private static String inputFilePath;
	private static boolean isOptimize = false;
	public static int totalNum = 0;
	public static int chunkSize = 0;

	public HBTree getHbtree() {
		return hbtree;
	}

	public HBTreeOptimize getHbtreeop() {
		return hbtreeop;
	}

	public static boolean isOptimize() {
		return isOptimize;
	}

	public HBTreeMultiThreadPutTest(HBTree hbtree, String inputFilePath) {
		this.hbtree = hbtree;
		this.inputFilePath = inputFilePath;
		this.chunkSize = hbtree.chunkSize;
	}

	public HBTreeMultiThreadPutTest(HBTreeOptimize hbtreeop,
			String inputFilePath) {
		this.hbtreeop = hbtreeop;
		this.inputFilePath = inputFilePath;
		this.chunkSize = hbtreeop.chunkSize;
		this.isOptimize = true;
	}

	@Override
	public void run() {
		String id = Thread.currentThread().getName();
		File file = new File(this.inputFilePath + id + ".txt");
		if (!file.exists()) {
			System.out.println("Input file is not found!");
			return;
		}
		try {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);
			if (this.isOptimize) {
				while (true) {
					String str = br.readLine();
					if (str == null) {
						break;
					}
					String[] line = str.split(" ");
					// Map<String,String> kvs = new HashMap<String,String>();
					for (int i = 1; i < line.length; i++) {
						this.hbtreeop.add(line[0], CommonVariable.COLUMN + i, line[i]);
						synchronized (HBTreeMultiThreadPutTest.class) {
							totalNum++;
						}
					}
				}
			} else {
				while (true) {
					String str = br.readLine();
					if (str == null) {
						break;
					}
					String[] line = str.split(" ");
					// Map<String,String> kvs = new HashMap<String,String>();
					for (int i = 1; i < line.length; i++) {
						this.hbtree.add(line[0], CommonVariable.COLUMN + i, line[i]);
						synchronized (HBTreeMultiThreadPutTest.class) {
							totalNum++;
						}
					}
				}
			}
			br.close();
			fr.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static long time = System.currentTimeMillis();
	static long startTime = time;
	static long lastNum = 0;
	static long targetNum = 0;
	public static Thread output = new Thread(new HBTreePutMetrics());
	
	public static class HBTreePutMetrics implements Runnable{
		@Override
		public void run() {
			FileWriter resultWriter = null;
			try {
				resultWriter = new FileWriter(CommonVariable.RESULT_FILE_PATH,
						true);
				String s = "\r\n\r\nOptimize = "
						+ HBTreeMultiThreadPutTest.isOptimize
						+ " , chunkSize = "
						+ HBTreeMultiThreadPutTest.chunkSize
						+ ", InputFilePath:"
						+ HBTreeMultiThreadPutTest.inputFilePath + "\r\n";
				resultWriter.write(s);
				System.out.print(s);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			StringBuilder r = new StringBuilder();
			while (true) {
				try {
					Thread.sleep(2000);
					long current = totalNum;
					r.delete(0, r.length());
					r.append("HBTreePut - StartTime:").append(time).append("  Now:").append(System.currentTimeMillis()).append("  putNum :")
					.append(current).append("  CurrentSpeed:").append(((current - lastNum) * 1000)
			            / (System.currentTimeMillis() - time)).append("  TotalSpeed:").append((current * 1000) / (System.currentTimeMillis() - startTime))
					 .append(" r/s");
					
					time = System.currentTimeMillis();
					System.out.println(r);
					resultWriter.write(r + "\r\n");
					if(current>=targetNum || current-lastNum==0 && (targetNum-current)<targetNum/100){
			        	resultWriter.close();
			        	break;
			        }
			        lastNum = current;
				} catch (Exception e) {
					e.printStackTrace();
					try {
						resultWriter.close();
					} catch (IOException s) {
						s.printStackTrace();
					}
				}
			}
		}
	}


	public static void main(String[] args) throws InterruptedException {
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

		String inputFilePath = "/ares/TestData/t2/keylen=32 columnNum=4/3000w/";
		int chunkSize = 12;
		int threadNum = 24 ;
		boolean optimize = false;
		int minLayerNum = 16;

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
		while(HBTreeMultiThreadPutTest.totalNum==0){
			Thread.sleep(200);
		}
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();
	}
}
