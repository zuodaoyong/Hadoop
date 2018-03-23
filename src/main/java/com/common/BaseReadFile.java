package com.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.functions.ReadFile;

public class BaseReadFile {

	private static Logger logger=LoggerFactory.getLogger(BaseReadFile.class);
	public static <T> Map<String,T> readFile(String dir,String suffix,ReadFile<T> readFile){
		final Map<String,T> map=new HashMap<>();
		try {
			URL url = BaseReadFile.class.getResource(dir);
			Path path = Paths.get(URI.create(url.toString()));
			Files.walkFileTree(path,  new FileVisitor<Path>(){
				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
					return FileVisitResult.CONTINUE;
				}
				
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					if (file.toString().endsWith(suffix)) {
					    String fileName=file.getFileName().toString();
					    fileName=fileName.substring(0,fileName.length()-4);
						FileInputStream fs=new FileInputStream(file.toFile());
					    try {
							map.put(fileName,readFile.read(fs));
						} catch (Exception e) {
							logger.error("BaseReadFile->readFile", e);
						}
	                } 
					return FileVisitResult.CONTINUE;
				}
				
				@Override
				public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
					return FileVisitResult.TERMINATE;
				}
				
				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					return FileVisitResult.TERMINATE;
				}
				
			});
		} catch (Exception e) {
			logger.error("BaseReadFile->readFile", e);
		}
		return map;
	}
}
