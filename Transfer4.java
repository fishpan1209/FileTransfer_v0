package com.fishpan1209;

/*
Transfer4:
directory creation: java io
header copying: java io
file copying: java nio byteBuffer 
*/


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;

public class Transfer4  {
	public Transfer4(){
		
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
				sendFile(srcDir, destDir);
				//System.out.println("File copied from " + srcDir + " to " + destDir);
			}
		}
	}
	
	public void sendFile(File srcDir, File destDir){
		System.out.println("Sending file: " + srcDir.getName());
		System.out.println(destDir.getPath());
		try {
			RandomAccessFile fromFile;
			fromFile = new RandomAccessFile(srcDir, "rw");
			FileChannel      fromChannel = fromFile.getChannel();

			RandomAccessFile toFile = new RandomAccessFile(destDir, "rw");
			FileChannel      toChannel = toFile.getChannel();

			long position = 0;
			long count    = fromChannel.size();

			fromChannel.transferTo(position, count, toChannel);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) throws IOException{
		String srcPath = "/users/aojing/dropbox/Liaison/Project/Data/corpus";
		String destPath = "/users/aojing/dropbox/Liaison/Project/output4";
		File srcDir = new File(srcPath);
		File destDir = new File(destPath);
		Transfer4 t4 = new Transfer4();
		long startTime = System.currentTimeMillis();
		t4.transferDir(srcDir, destDir);
		long endTime = System.currentTimeMillis();
        System.out.println("Total Time is  " + (endTime - startTime)+"ms");
		
	}
}
