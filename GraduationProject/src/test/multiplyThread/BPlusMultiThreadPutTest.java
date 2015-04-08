package test.multiplyThread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import common.Value;
import test.CommonVariable;
import tree.bplus.BPlus;

public class BPlusMultiThreadPutTest implements Runnable{
	private BPlus bskl;
	private static String inputFilePath;
	public static int totalNum = 0;
	
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
	static long targetNum = 0;
	public static Thread output =  new Thread(new BPlusPutMetrics());
	
	public static class BPlusPutMetrics implements Runnable{
		@Override
		public void run() {
			FileWriter resultWriter = null;
			try {
				resultWriter = new FileWriter(CommonVariable.RESULT_FILE_PATH,true);
				String s = "\r\n\r\n\r\n"
						+ "InputFilePath:"
						+ BPlusMultiThreadPutTest.inputFilePath + "\r\n";
				resultWriter.write(s);
				System.out.print(s);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			StringBuilder r = new StringBuilder();
			while(true){
				try {
					Thread.sleep(2000);
					long current = totalNum;
					r.delete(0, r.length());
					r.append("BPlusPut - StartTime:").append(time).append("  Now:").append(System.currentTimeMillis()).append("  putNum :")
					.append(current).append("  CurrentSpeed:").append(((current - lastNum) * 1000)
			            / (System.currentTimeMillis() - time)).append("  TotalSpeed:").append((current * 1000) / (System.currentTimeMillis() - startTime))
					 .append(" r/s");
			        time = System.currentTimeMillis();
			        System.out.println(r);
			        resultWriter.write(r+"\r\n");
			        if(current>=targetNum || current-lastNum==0 && (targetNum-current)<targetNum/100){
			        	resultWriter.flush();
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
	
	public static void main(String[] args) throws InterruptedException, IOException{
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
		
		int keylen[] = {8,16,24,32};
		int threadNum[] ={8,16,24,32}; 
		
		for(int i=1;i<=5;i++){
			for(int len:keylen){
				for(int tn:threadNum){
					String inputFilePath = "/ares/TestData/t2/thread="+tn+"/keylen="+len+" columnNum=4/"+1000*i+"w/";
					
					BPlus bp = new BPlus(c);
					BPlusMultiThreadPutTest btp = new BPlusMultiThreadPutTest(bp,inputFilePath);
					
					System.gc();
					InputStreamReader isReader = new InputStreamReader(System.in);
					String str = new BufferedReader(isReader).readLine();
					System.out.println("Input String: "+str);
					
					for(int j=0;i<tn;i++){
						Thread t =  new Thread(btp);
						t.setName(String.valueOf(j));
						t.start();
					}
					while(BPlusMultiThreadPutTest.totalNum==0){
						Thread.sleep(200);
					}
					BPlusMultiThreadPutTest.time = System.currentTimeMillis();
					BPlusMultiThreadPutTest.startTime = BPlusMultiThreadPutTest.time;
					BPlusMultiThreadPutTest.targetNum = i*1000*10000;
					output.setPriority(Thread.MAX_PRIORITY);
					output.start();
					output.join();
				}
			}
		}
	}
}
