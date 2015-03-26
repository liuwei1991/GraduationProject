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
	private static int totalNum = 0;
	public static int chunkSize = 0;

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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static long time = System.currentTimeMillis();
	static long startTime = time;
	static long lastNum = 0;

	public static Thread output = new Thread() {
		public void run() {
			FileWriter resultWriter = null;
			try {
				resultWriter = new FileWriter(CommonVariable.RESULT_FILE_PATH,
						true);
				resultWriter.write("\r\n\r\nOptimize = "
						+ HBTreeMultiThreadPutTest.isOptimize
						+ " , chunkSize = "
						+ HBTreeMultiThreadPutTest.chunkSize
						+ ", InputFilePath:"
						+ HBTreeMultiThreadPutTest.inputFilePath + "\r\n");
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			String r = null;
			while (true) {
				try {
					sleep(2000);
					long current = totalNum;
					r = "HBTreePut - StartTime:" + time + "  Now:"
							+ System.currentTimeMillis() + "  putNum :"
							+ current + "  CurrentSpeed:"
							+ ((current - lastNum) * 1000)
							/ (System.currentTimeMillis() - time)
							+ "  TotalSpeed:" + (current * 1000)
							/ (System.currentTimeMillis() - startTime) + " r/s";
					time = System.currentTimeMillis();
					lastNum = current;
					System.out.println(r);
					resultWriter.write(r + "\r\n");
					resultWriter.flush();
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
	};

	public static void main(String[] args) {
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

		String inputFilePath = "/TestData/t2/keylen=16 columnNum=1/500w/";
		int chunkSize = 6;
		int threadNum = 10;
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
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();
	}
}
