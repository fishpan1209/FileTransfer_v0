package com.fishpan1209;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TransferEvent {
	
	public TransferEvent(){
		
	}

	public static void main(String[] args) throws IOException {
		if(args.length<2){
			throw  new IllegalArgumentException("Not enough args");
		}
		System.out.println("Start transfering files ...\n");
		
		String srcPath = args[0];
		File srcDir = new File(srcPath);
		
		// transfer1
		String destPath1 = args[1]+"/output1";
		File destDir1 = new File(destPath1);
		
		
		// transfer1
		Transfer1 t1 = new Transfer1();
		long startTime = System.currentTimeMillis();
		t1.transferDir(srcDir, destDir1);
		long endTime = System.currentTimeMillis();
        System.out.println("Total Time of transfer1 is  " + (endTime - startTime)+"ms");
        
        // transfer2
        String destPath2 = args[1]+"/output2";
		File destDir2 = new File(destPath2);
        Transfer2 t2 = new Transfer2();
		startTime = System.currentTimeMillis();
		t2.transferDir(srcDir, destDir2);
		endTime = System.currentTimeMillis();
        System.out.println("Total Time of transfer2 is  " + (endTime - startTime)+"ms");
        
        /*
        // transfer3
        String destPath3 = args[1]+"/output3";
		File destDir3 = new File(destPath3);
        Transfer3 t3 = new Transfer3();
		startTime = System.currentTimeMillis();
		t3.transferDir(Paths.get(srcPath), Paths.get(destPath3));
		endTime = System.currentTimeMillis();
        System.out.println("Total Time of transfer3 is " + (endTime - startTime)+"ms");
        */
        // transfer4
        String destPath4 = args[1]+"/output4";
		File destDir4 = new File(destPath4);
        Transfer4 t4 = new Transfer4();
		startTime = System.currentTimeMillis();
		t4.transferDir(srcDir, destDir4);
		endTime = System.currentTimeMillis();
        System.out.println("Total Time of transfer4 is  " + (endTime - startTime)+"ms");
        

	}

}
