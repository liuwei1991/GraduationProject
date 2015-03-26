package test.multiplyThread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import common.Value;
import test.CommonVariable;
import tree.bplus.BPlus;

public class BPlusMultiThreadPutTest implements Runnable{
	private BPlus bskl;
	private static String inputFilePath;
	private static int totalNum = 0;
	
	public BPlusMultiThreadPutTest(BPlus bskl,String inputFilePath){
		this.bskl = bskl;
		this.inputFilePath = inputFilePath;
	}

	@Override
	public void run() {
		String id = Thread.currentThread().getName();
		File file = new File(this.inputFilePath+id+".txt");
		if(!file.exists()){
			System.out.println("Input file is not found!");
			return;
		}
		try{
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);
			while(true){
				String str = br.readLine();
				if(str==null){
					break;
				}
				String[] line = str.split(" ");
//				Map<String,String> kvs = new HashMap<String,String>();
				
				for(int i=1;i<line.length;i++){
					this.bskl.add(line[0],CommonVariable.COLUMN+i, line[0]+","+line[i] );
					synchronized(BPlusMultiThreadPutTest.class){
						totalNum++;
					}
				}
			}
			br.close();
			fr.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	
	static long time = System.currentTimeMillis();
	static long startTime = time;
	static long lastNum = 0;
	
	public static Thread output =  new Thread(){
		public void run(){
			FileWriter resultWriter = null;
			try {
				resultWriter = new FileWriter(CommonVariable.RESULT_FILE_PATH,true);
				resultWriter.write("\r\n\r\n\r\n"
						+ "InputFilePath:"
						+ BPlusMultiThreadPutTest.inputFilePath + "\r\n");
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			String r = null;
			while(true){
				try {
					sleep(2000);
					long current = totalNum;
					r= "BPlusPut - StartTime:" + time + "  Now:"
			            + System.currentTimeMillis() + "  putNum :" + current
			            + "  CurrentSpeed:" + ((current - lastNum) * 1000)
			            / (System.currentTimeMillis() - time) + "  TotalSpeed:"
			            + (current * 1000) / (System.currentTimeMillis() - startTime)
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
	
	public static void main(String[] args) throws InterruptedException{
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
		BPlusMultiThreadPutTest btp = new BPlusMultiThreadPutTest(bp,inputFilePath);
		for(int i=0;i<threadNum;i++){
			Thread t =  new Thread(btp);
			t.setName(String.valueOf(i));
			t.start();
		}
		while(BPlusMultiThreadPutTest.totalNum==0){
			Thread.sleep(200);
		}
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();
	}
}
