/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lafs.hdfs;

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
public class TestSeek {
	
	public static void main(String[] args) throws IOException {
		
		LogManager.getLogManager().getLogger("").setLevel(Level.INFO);
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "lafs://localhost:3456");
		conf.set("fs.lafs.impl", "org.lafs.hdfs.LAFS");
		conf.set("lafs.rootcap", "URI:DIR2:ruhtwdg25dki5r3mgsqefonnrm:pbv73fb7jqtborelpsaih6awzveteiejympzgfpijb4mieqnpo5q");
				
		
		FileSystem fs = FileSystem.get(conf);
		
		Path testfilePath = new Path("/testfile");
		
		if(fs.exists(testfilePath))
			fs.delete(testfilePath, true);
		
		FSDataOutputStream output = fs.create(testfilePath);
		output.writeUTF("this is a test.");
		System.out.println(output.getPos());
		
		output.writeUTF("another test here.");
		System.out.println(output.getPos());
		output.close();
		
		FSDataInputStream input = fs.open(testfilePath);
		String one = input.readUTF();
		System.out.println(one);
		
		long pos = input.getPos();
		System.out.println(pos);
		
		String two = input.readUTF();
		System.out.println(two);
		
		input.seek(0);
		String three = input.readUTF();
		
		System.out.println(three);
		
		fs.delete(testfilePath, true);
		
		System.out.println(fs.exists(testfilePath));
	}
}
