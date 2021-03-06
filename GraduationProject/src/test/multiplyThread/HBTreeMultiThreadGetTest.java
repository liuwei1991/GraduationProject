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
import tree.hb.HBTree;
import tree.hb.HBTreeOptimize;
import common.Value;

public class HBTreeMultiThreadGetTest implements Runnable {
	private HBTree hbtree;
	private static String inputFilePath;
	private int threadNum;
	public static int totalNum = 0;
	private HBTreeOptimize hbtreeop;
	private static boolean isOptimize = false;
	public static int chunkSize = 0;

	public HBTreeMultiThreadGetTest(HBTree hbtree, String inputFilePath,
			int threadNum) {
		this.hbtree = hbtree;
		this.inputFilePath = inputFilePath;
		this.threadNum = threadNum;
		this.chunkSize = hbtree.chunkSize;
		this.isOptimize = false;
	}

	public HBTreeMultiThreadGetTest(HBTreeOptimize hbtreeop,
			String inputFilePath, int threadNum) {
		this.hbtreeop = hbtreeop;
		this.inputFilePath = inputFilePath;
		this.threadNum = threadNum;
		this.chunkSize = hbtreeop.chunkSize;
		this.isOptimize = true;
	}

	public void put() throws IOException {
		if (this.isOptimize) {
			this.putOptimize();
		} else {
			this.putOrdinary();
		}
	}

	public void putOrdinary() throws IOException {
		for (int i = 0; i < this.threadNum; i++) {
			File file = new File(this.inputFilePath + i + ".txt");
			if (!file.exists()) {
				System.out.println("Input file is not found!");
				return;
			}
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);

			while (true) {
				String str = br.readLine();
				if (str == null) {
					break;
				}
				String[] line = str.split(" ");
				// Map<String,String> kvs = new HashMap<String,String>();
				for (int j = 1; j < line.length; j++) {
					// kvs.put(column+j, line[j]);
					this.hbtree.add(line[0], CommonVariable.COLUMN + j, line[j]);
				}
			}
			br.close();
			fr.close();
		}
	}

	public void putOptimize() throws IOException {
		for (int i = 0; i < this.threadNum; i++) {
			File file = new File(this.inputFilePath + i + ".txt");
			if (!file.exists()) {
				System.out.println("Input file is not found!");
				return;
			}
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);

			while (true) {
				String str = br.readLine();
				if (str == null) {
					break;
				}
				String[] line = str.split(" ");
				// Map<String,String> kvs = new HashMap<String,String>();
				for (int j = 1; j < line.length; j++) {
					this.hbtreeop.add(line[0], CommonVariable.COLUMN + j, line[j]);
				}
			}
			br.close();
			fr.close();
		}
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
			if(this.isOptimize){
				while (true) {
					String str = br.readLine();
					if (str == null) {
						break;
					}
					String[] lines = str.split(" ");
					for (int j = 1; j < lines.length; j++) {
						this.hbtreeop.get(lines[0], CommonVariable.COLUMN + j);
						synchronized (HBTreeMultiThreadGetTest.class) {
							totalNum++;
						}
					}
				}
			}else{
				while (true) {
					String str = br.readLine();
					if (str == null) {
						break;
					}
					String[] lines = str.split(" ");
					for (int j = 1; j < lines.length; j++) {
						this.hbtree.get(lines[0], CommonVariable.COLUMN + j);
						synchronized (HBTreeMultiThreadGetTest.class) {
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
	static long targetNum = 0;
	
	public static class HBTreeGetMetrics implements Runnable{
		@Override
		public void run() {
			FileWriter resultWriter = null;
			try {
				resultWriter = new FileWriter(CommonVariable.RESULT_FILE_PATH,
						true);
				String r = "\r\n\r\nOptimize = "
						+ HBTreeMultiThreadGetTest.isOptimize
						+ " , chunkSize = "
						+ HBTreeMultiThreadGetTest.chunkSize
						+ ", InputFilePath:"
						+ HBTreeMultiThreadGetTest.inputFilePath + "\r\n";
				resultWriter.write(r);
				System.out.print(r);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			StringBuilder r = new StringBuilder();
			while (true) {
				try {
					Thread.sleep(2000);
					long current = totalNum;
					r.delete(0, r.length());
					r.append("HBTreeGet - StartTime:").append(time).append("  Now:").append(System.currentTimeMillis()).append("  getNum :")
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
	
	public static Thread output = new Thread(new HBTreeGetMetrics());

	public static void main(String[] args) throws IOException, InterruptedException {
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
		int chunkSize = 4;
		int threadNum = 10;
		boolean optimize = false;
		int minLayerNum = 16;

		HBTreeMultiThreadGetTest hbtg = null;
		if (optimize) {
			HBTreeOptimize hbtreeop = new HBTreeOptimize(c, chunkSize,
					minLayerNum);
			hbtg = new HBTreeMultiThreadGetTest(hbtreeop, inputFilePath,
					threadNum);
		} else {
			HBTree hbtree = new HBTree(c, chunkSize);
			hbtg = new HBTreeMultiThreadGetTest(hbtree, inputFilePath,
					threadNum);
		}

		hbtg.put();
		for (int i = 0; i < threadNum; i++) {
			Thread t = new Thread(hbtg);
			t.setName(String.valueOf(i));
			t.start();
		}
		while(HBTreeMultiThreadGetTest.totalNum==0){
			Thread.sleep(200);
		}
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();
	}
}
