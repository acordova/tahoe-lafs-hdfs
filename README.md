tahoe-lafs-hdfs
===============

Adapter for Hadoop DFS clients to store data in Tahoe LAFS (https://tahoe-lafs.org/trac/tahoe-lafs)

This allows applications such as Hadoop MapReduce and other technologies that are designed for HDFS to run on Tahoe-LAFS, an encrypted, distributed file system, with no modifications to the application code.

This code has been tested with Hadoop 2.2.0 and Apache Accumulo 1.6.0. A demonstration video is available at https://www.youtube.com/watch?v=7KLGuJro-i8

Building
--------
This code can be built using maven via:

  
  mvn package
  

Configuration
-------------
The only modifications required to applications designed for HDFS include placing the resulting JAR file into the Java classpath and using the following settings in core-site.xml

	<property>
		<name>fs.defaultFS</name>
		<value>lafs://localhost:3456</value>
	</property> 
  
	<property>
		<name>fs.lafs.impl</name>
		<value>org.lafs.hdfs.LAFS</value>
	</property>
	
	<property>
		<name>lafs.rootcap</name>
		<value>[rootcap goes here]</value>
	</property>
	
	<property>
		<name>lafs.write_chunk_size</name>
		<value>20000000</value>
	</property>


Benefits
--------
Users gain protection against unauthorized data access in the case of theft of physical hard drives. Additional security gains are possible with additional security measures and depend on the trust placed in various hardware components.

