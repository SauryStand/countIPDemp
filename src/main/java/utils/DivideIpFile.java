package com.mycodes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class DivideIpFile {

	protected final static int HASH_NUM = 1000;
	protected final static String FILE_NAME = "D:/Source_codes/My_java_projects/ipDivide/temp1/ip_sources.txt";   
	protected final static String FOLDER = "D:/Source_codes/My_java_projects/ipDivide/temp1/file_set2/";   
	
	public static void main(String[] args) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		System.out.println("-->>analysis file:start,current time is: " + df.format(new Date()));
		//calculate();
		divideIpsFile();
		System.out.println("-->>analysis file:end, current time is: " + df.format(new Date()));
	}
	
	private static String hash(String ip) {
		long numIp = ipToLong(ip);
		return String.valueOf(numIp % HASH_NUM);
	}

	/**
	 * 在这里采取的是就是Hash取模的方式，将字符串的ip地址给转换成一个长整数，并将这个数对1000取模，将模一样的ip放到同一个文件，这样就能够生成1000个小文件，每个文件就只有1M多，在这里已经是足够小的了。
	 * @param strIp
	 * @return
	 */
	private static long ipToLong(String strIp) {
		long[] ip = new long[4];
		int position1 = strIp.indexOf(".");
		int position2 = strIp.indexOf(".", position1 + 1);
		int position3 = strIp.indexOf(".", position2 + 1);

		ip[0] = Long.parseLong(strIp.substring(0, position1));
		ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
		ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
		ip[3] = Long.parseLong(strIp.substring(position3 + 1));
		return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
	}	
	
	protected static void divideIpsFile() {
		File file = new File(FILE_NAME);
		Map<String, StringBuilder> map  = new HashMap<String,StringBuilder>();
		int count = 0;
		try {
			FileReader fileReader = new FileReader(file);
			BufferedReader br = new BufferedReader(fileReader);
			String ip;
			while ((ip = br.readLine()) != null) {
				String hashIp = hash(ip);
				if(map.containsKey(hashIp)){
					StringBuilder sb = (StringBuilder)map.get(hashIp);
					sb.append(ip).append("\n");
					map.put(hashIp, sb);
				}else{
					StringBuilder sb = new StringBuilder(ip);
					sb.append("\n");
					map.put(hashIp, sb);
				}
				count++;
				if(count == 4000000){
					Iterator<String> it = map.keySet().iterator();					
					while(it.hasNext()){
						String fileName = it.next();
						File ipFile = new File(FOLDER + "/" + fileName + ".txt");
						FileWriter fileWriter = new FileWriter(ipFile, true);
						StringBuilder sb = map.get(fileName);				
						fileWriter.write(sb.toString());
						fileWriter.close();
					}
					count = 0;
					map.clear();
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	/**
	 * start calculate the util file
	 */
	protected static void calculate() {  
		Map<String, Integer> map = new HashMap<String, Integer>(); 
		Map<String, Integer> finalMap = new HashMap<String, Integer>(); 
	    File folder = new File(FOLDER);  
	    File[] files = folder.listFiles();  
	    FileReader fileReader;  
	    BufferedReader br;  
	    for (File file : files) {  
	        try {  
	            fileReader = new FileReader(file);  
	            br = new BufferedReader(fileReader);  
	            String ip;  
	            Map<String, Integer> tmpMap = new HashMap<String, Integer>();  
	            while ((ip = br.readLine()) != null) {  
	                if (tmpMap.containsKey(ip)) {  
	                    int count = tmpMap.get(ip);  
	                    tmpMap.put(ip, count + 1);  
	                } else {  
	                    tmpMap.put(ip, 0);  
	                }  
	            }     
	            fileReader.close();  
	            br.close();  
	            countIpQuantities(tmpMap,map);  
	            tmpMap.clear();  
	        } catch (FileNotFoundException e) {  
	            e.printStackTrace();  
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
	    }  
	      
	    countIpQuantities(map,finalMap);          
	    Iterator<String> it = finalMap.keySet().iterator();  
	    while(it.hasNext()){  
	        String ip = it.next();  
	        System.out.println("result IP : " + ip + " | count = " + finalMap.get(ip));  
	    }  
	      
	}         
	  
	private static void countIpQuantities(Map<String, Integer> pMap, Map<String, Integer> resultMap) {  
	    Iterator<Entry<String, Integer>> it = pMap.entrySet().iterator();  
	    int max = 0;  
	    String resultIp = "";  
	    while (it.hasNext()) {  
	        Entry<String, Integer> entry = (Entry<String, Integer>) it.next();  
	        if (entry.getValue() > max) {  
	            max = entry.getValue();  
	            resultIp = entry.getKey();  
	        }  
	    }  
	    resultMap.put(resultIp,max);      
	}	
	

}
