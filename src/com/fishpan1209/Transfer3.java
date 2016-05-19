package com.fishpan1209;
/*
Transfer3:
directory creation: java io
header copying: java io
file copying: java nio byteBuffer 
*/


import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
public class Transfer3  {
	public Transfer3(){
		
	}
	
	public void transferDir(Path srcDir, Path destDir) throws IOException { 
		    
			Files.walkFileTree(srcDir, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
		     new SimpleFileVisitor<Path>() {
		        @Override
		        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException  {
		            Path target = destDir.resolve(srcDir.relativize(dir));
		            try {
		                Files.copy(dir, target);
		            } catch (FileAlreadyExistsException e) {
		                 if (!Files.isDirectory(target))
		                     throw e;
		            }
		            return FileVisitResult.CONTINUE;
		        }
		        @Override
		        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
		            Files.copy(file, destDir.resolve(srcDir.relativize(file)));
		            return FileVisitResult.CONTINUE;
		        }
	    });
	
	}
/*	
	public static void main(String[] args) throws IOException{
		String srcPath = "/users/aojing/dropbox/Liaison/Project/Data/corpus/";
		String destPath = "/users/aojing/dropbox/Liaison/Project/output3";
		Path srcDir = Paths.get(srcPath);
		Path destDir = Paths.get(destPath);
		Transfer3 t3 = new Transfer3();
		long startTime = System.currentTimeMillis();
		t3.transferDir(srcDir, destDir);
		long endTime = System.currentTimeMillis();
        System.out.println("Total Time of transfer3 is " + (endTime - startTime)+"ms");
		
	}
	*/
}
