package test.multiplyThread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import common.Value;
import tree.bplus.BPlus;

public class BPlusMultiThreadGetTest implements Runnable {

	private BPlus bskl;
	private String inputFilePath;
	private int threadNum;
	private static int totalNum = 0;
	
	public BPlusMultiThreadGetTest(BPlus bskl, String inputFilePath,int threadNum) {
		this.bskl = bskl;
		this.inputFilePath = inputFilePath;
		this.threadNum = threadNum;
	}
	
	public void put() throws IOException{
		for(int i=0;i<this.threadNum;i++){
			File file = new File(this.inputFilePath+i+".txt");
			if(!file.exists()){
				System.out.println("Input file is not found!");
				return;
			}
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			
			while(true){
				String str = br.readLine();
				if(str==null){
					break;
				}
				String[] line = str.split(" ");
				String column = "q1";
				Map<String,String> kvs = new HashMap<String,String>();
				kvs.put(column, line[1]);
				this.bskl.add(line[0],kvs);
			}
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
			BufferedReader br = new BufferedReader(fr);

			while (true) {
				String str = br.readLine();
				// if reach the end of the file, the start from the begin of the file.
				if (str == null) {
					fr = new FileReader(file);
					br = new BufferedReader(fr);
					continue;
//					break;
				}
				String key = str.split(" ")[0];
				String column = "q1";
				this.bskl.get(key,column);
				synchronized (BPlusMultiThreadGetTest.class) {
					totalNum++;
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
			while (true) {
				try {
					sleep(2000);
					long current = totalNum;
					System.out
							.println("BPlusGet - StartTime:" + time + " Now:"
									+ System.currentTimeMillis() + " getNum :"
									+ current + " CurrentSpeed:"
									+ ((current - lastNum) * 1000)
									/ (System.currentTimeMillis() - time)
									+ "  TotalSpeed:" + (current * 1000)
									/ (System.currentTimeMillis() - startTime)
									+ " r/s");
					time = System.currentTimeMillis();
					lastNum = current;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	};
	static {
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();
	}

	public static void main(String[] args) throws IOException {
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

		String inputFilePath = "D:/TestData/t2/keylen=16/500w/";
		int threadNum = 10;

		BPlus bp = new BPlus(c);
		BPlusMultiThreadGetTest btg = new BPlusMultiThreadGetTest(bp,
				inputFilePath,threadNum);
		btg.put();
		for (int i = 0; i < threadNum; i++) {
			Thread t = new Thread(btg);
			t.setName(String.valueOf(i));
			t.start();
		}
	}

}
