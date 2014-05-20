/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lafs.hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author aaron
 */
public class TestLargeFile {
	
	public static void main(String[] args) throws IOException {
		
		LogManager.getLogManager().getLogger("").setLevel(Level.INFO);
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "lafs://localhost:3456");
		conf.set("fs.lafs.impl", "org.lafs.hdfs.LAFS");
		conf.set("lafs.rootcap", "URI:DIR2:ruhtwdg25dki5r3mgsqefonnrm:pbv73fb7jqtborelpsaih6awzveteiejympzgfpijb4mieqnpo5q");
				
		
		FileSystem fs = FileSystem.get(conf);
		
		// try writing a large file
		Path testlargefilePath = new Path("/testlargefile");
		
		byte[] chars = new byte[1024];
		//for(int i=0; i < 1024; i++)
		//	chars[i] = 41;
		//String byteString = new String(chars);
		
		//FSDataOutputStream output = fs.create(testlargefilePath);
		// write about GB of data
		//for(int i=0; i < 1024 * 1024; i++) 
		//	output.writeBytes(byteString);
		
		//output.close();
		
		long start = System.currentTimeMillis();
		FSDataInputStream input = fs.open(testlargefilePath);
		int bytesRead = 0;
		while(true) {
			try {
				input.readFully(chars);
				bytesRead += 1024;
			}
			catch (EOFException eofe) {
				break;
			}
		}
		
		System.out.println("Bytes read: " + bytesRead + ". " + (bytesRead / (System.currentTimeMillis() - start)) + "bpms");
	}
}
