package sPreprocess.sTool;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;

public class CountDegree extends Configured {

    public static void WriteFile(String hdfs, String message) throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(hdfs), conf);
	FSDataOutputStream hdfsOutStream = fs.create(new Path(hdfs));
	hdfsOutStream.writeChars(message);
	hdfsOutStream.close();
	fs.close();
    }

    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf();

	// conf.set("mapred.job.tracker", "local"); conf.set("fs.defaultFS",
	// "file:///");

	long l3 = System.currentTimeMillis();
	// Run map1 and reduce1
	int exitCode = ToolRunner.run(conf, new DegreeCounterA(), args);

	// Run map2 and reduce2
	exitCode = ToolRunner.run(conf, new DegreeCounterB(), args);
	l3 = System.currentTimeMillis() - l3;
	// WriteFile(args[2]+"/time", Long.toString(l3));

	System.exit(exitCode);
    }

}
