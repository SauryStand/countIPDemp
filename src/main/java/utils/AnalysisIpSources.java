package com.mycodes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AnalysisIpSources {
	
	public static void main(String[] args){
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long startTime = System.currentTimeMillis();
		System.out.println("-->>Create file: start, current time is: " + df.format(new Date()));
		//generateIpsFile();
		System.out.println("-->>Create file: end, current time is: " + df.format(new Date()));
		System.out.println("-->>Mapping large files into small files, start...");
		DivideIpFile.divideIpsFile();
		System.out.println("-->>Mapping large files into small files, end. Total time cost£º" + getMS(startTime) + " millisecond");  
		System.out.println("-->>Analysing all small files, start...");
		DivideIpFile.calculate();
		System.out.println("-->>Analysing all small files, end. " + getMS(startTime) + " millisecond");
		System.out.println("-->>Total time cost£º" + getMS(startTime) + "millisecond");  
		
	}
	
	/**
	 * GET MILLISECOND
	 * @return
	 */
	private static long getMS(long startTime){
		long end = System.currentTimeMillis(); 
		return (end - startTime);
	}

	private static String generateIp() {
		return "192.168." + (int) (Math.random() * 255) + "." + (int) (Math.random() * 255) + "\n";
	}
	
	/**
	 * create local ip file
	 */
	private static void generateIpsFile() {
		File file = new File(DivideIpFile.FILE_NAME);
		try {
			FileWriter fileWriter = new FileWriter(file);
			for (int i = 0; i < 100000000; i++) {
				fileWriter.write(generateIp());
			}
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	
	
}
