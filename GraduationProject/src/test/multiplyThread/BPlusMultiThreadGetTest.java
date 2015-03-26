package test.multiplyThread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import common.Value;
import test.CommonVariable;
import tree.bplus.BPlus;

public class BPlusMultiThreadGetTest implements Runnable {

	private BPlus bskl;
	private static String inputFilePath;
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
			BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);
			
			while(true){
				String str = br.readLine();
				if(str==null){
					break;
				}
				String[] line = str.split(" ");
//				Map<String,String> kvs = new HashMap<String,String>();
				for(int j=1;j<line.length;j++){
					this.bskl.add(line[0],"column"+j, line[0]+","+line[j] );
				}
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
			BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);

			while (true) {
				String str = br.readLine();
				// if reach the end of the file, the start from the begin of the file.
				if (str == null) {
					fr = new FileReader(file);
					br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);
					continue;
//					break;
				}
				String[] line = str.split(" ");
				String column = "q";
				for(int i=1;i<line.length;i++){
					this.bskl.get(line[0], column+i);
					synchronized (BPlusMultiThreadGetTest.class) {
						totalNum++;
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
				resultWriter = new FileWriter(CommonVariable.RESULT_FILE_PATH,true);
				resultWriter.write("\r\n\r\n\r\n"
						+ "InputFilePath:"
						+ BPlusMultiThreadGetTest.inputFilePath + "\r\n");
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			String r = null;
			while (true) {
				try {
					sleep(2000);
					long current = totalNum;
					r = "BPlusGet - StartTime:" + time + " Now:"
									+ System.currentTimeMillis() + " getNum :"
									+ current + " CurrentSpeed:"
									+ ((current - lastNum) * 1000)
									/ (System.currentTimeMillis() - time)
									+ "  TotalSpeed:" + (current * 1000)
									/ (System.currentTimeMillis() - startTime)
									+ " r/s";
					time = System.currentTimeMillis();
					lastNum = current;
					System.out.println(r);
					resultWriter.write(r+"\r\n");
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
		while(BPlusMultiThreadGetTest.totalNum==0){
			Thread.sleep(200);
		}
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();
	}
}
