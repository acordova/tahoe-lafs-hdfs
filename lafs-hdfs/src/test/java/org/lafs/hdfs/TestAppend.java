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
public class TestAppend {
	
	public static void main(String[] args) throws IOException {
		
		LogManager.getLogManager().getLogger("").setLevel(Level.INFO);
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "lafs://localhost:3456");
		conf.set("fs.lafs.impl", "org.lafs.hdfs.LAFS");
		conf.set("lafs.rootcap", "URI:DIR2:ruhtwdg25dki5r3mgsqefonnrm:pbv73fb7jqtborelpsaih6awzveteiejympzgfpijb4mieqnpo5q");
				
		
		FileSystem fs = FileSystem.get(conf);
		
		Path testfilePath = new Path("/testfile");
		
		// read after close
		FSDataOutputStream output = fs.create(testfilePath);
		output.writeUTF("this is a test string. there are many like it but this one is mine.");
		output.close();
		
		FSDataInputStream input = fs.open(testfilePath);
		String s = input.readUTF();
		
		System.out.println(s);
		
		output = fs.append(testfilePath);
		output.writeUTF("This is some additional information");
		output.close();
		
		input = fs.open(testfilePath);
		while(true) {
			try {
				s = input.readUTF();
				System.out.println(s);
			} catch(EOFException eofe) {
				break;
			}
		}
	}
}
