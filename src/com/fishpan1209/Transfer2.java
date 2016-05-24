package com.fishpan1209;
/*
Transfer2:
directory creation: java io
header copying: java io
file copying: java io input/output stream 
*/
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;


public class Transfer2 {
	private String srcPath;
	private String destPath;
	
	public Transfer2(){
		
	}
	
	public Transfer2(String srcPath, String destPath){
		this.srcPath = srcPath;
		this.destPath = destPath;
	}
	
	public void transferDir(File srcDir, File destDir) {
		// make sure source exists
		if (!srcDir.exists()) {
			System.out.println("Directory does not exist.");
			System.exit(0);
		} 
		else {
			if (srcDir.isDirectory()) {
				// if destination directory not exists, create it
				if (!destDir.exists()) {
					destDir.mkdir();
				}

				// list all the directory contents
				String files[] = srcDir.list();

				for (String file : files) {
					// construct the src and dest file structure
					File srcFile = new File(srcDir, file);
					File destFile = new File(destDir, file);
					// recursive copy
					transferDir(srcFile, destFile);
				}
			} 
			else {
				// if file, then copy it
				//File file = new File(srcDir.getAbsolutePath());
				ProcessFile processer = new ProcessFile(srcDir.toPath());
				String header = processer.getHeader();
				sendFile(srcDir, header, destDir);
				//System.out.println("File copied from " + srcDir + " to " + destDir);
			}
		}
	}

	public void sendFile(File file, String header, File destDir){
		try {
			//System.out.println("Sending file: " + file.getName());
			// create directory for new file
			
			try {
				InputStream in = new FileInputStream(file);
				OutputStream out = new FileOutputStream(destDir);
				// write header
				byte[] headerByte = header.getBytes();
				out.write(headerByte);

				// copy file content in bytes
				byte[] buffer = new byte[8196];
				int length;
				while ((length = in.read(buffer)) > 0) {
					out.write(buffer, 0, length);
				}
				
				in.close();
				out.close();
				//System.out.println("End of file reached.." + "\n");

			} catch (FileNotFoundException e) {
				System.out.println("FILE NOT FOUND EXCEPTION");
				e.getMessage();
			}
		} catch (IOException e) {
			System.out.println("IO EXCEPTION");
			e.getMessage();
		}
	}
/*	
	public static void main(String[] args){
		String srcPath = "/users/aojing/dropbox/Liaison/Project/Data/corpus/";
		String destPath = "/users/aojing/dropbox/Liaison/Project/output2";
		File srcDir = new File(srcPath);
		File destDir = new File(destPath);
		Transfer2 t2 = new Transfer2();
		long startTime = System.currentTimeMillis();
		t2.transferDir(srcDir, destDir);
		long endTime = System.currentTimeMillis();
        System.out.println("Total Time of transfer2 is  " + (endTime - startTime)+"ms");
	}
*/
}

