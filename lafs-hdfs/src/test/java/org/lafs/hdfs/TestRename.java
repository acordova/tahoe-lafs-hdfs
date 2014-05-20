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
public class TestRename {
	
	public static void main(String[] args) throws IOException {
		
		LogManager.getLogManager().getLogger("").setLevel(Level.INFO);
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "lafs://localhost:3456");
		conf.set("fs.lafs.impl", "org.lafs.hdfs.LAFS");
		conf.set("lafs.rootcap", "URI:DIR2:ruhtwdg25dki5r3mgsqefonnrm:pbv73fb7jqtborelpsaih6awzveteiejympzgfpijb4mieqnpo5q");
				
		
		FileSystem fs = FileSystem.get(conf);
		
		//fs.mkdirs(new Path("/a/b/c"));
		
		
		//Path testfilePath = new Path("/a/b/c/testfile");
		Path testfilePath = new Path("lafs://localhost:3456/accumulo/tables/!0/table_info/F0000000.rf_tmp");
		
		
		//if(fs.exists(testfilePath))
		//	fs.delete(testfilePath, true);
		
		//FSDataOutputStream output = fs.create(testfilePath);
		//output.writeUTF("this is a test.");
		//output.close();
		
		
		Path renamedPath = new Path("lafs://localhost:3456/accumulo/tables/!0/table_info/F0000000.rf");
		System.out.println(fs.rename(testfilePath, renamedPath));
		
		//FSDataInputStream input = fs.open(renamedPath);
		//String read = input.readUTF();
		//System.out.println(read);
		
		//fs.delete(renamedPath, true);
	}
}
