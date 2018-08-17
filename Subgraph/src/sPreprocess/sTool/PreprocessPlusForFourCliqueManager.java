package sPreprocess.sTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

public class PreprocessPlusForFourCliqueManager extends Configured {

    public static void main(String[] args) {
	Configuration conf = new Configuration();
	/*
	 * conf.set("mapred.job.tracker", "local"); conf.set("fs.defaultFS",
	 * "file:///");
	 * 
	 */
	conf.set("mapreduce.reduce.memory.mb", "4096");
	// Run map1 and reduce1
	int exitCode = 0;
	try {
	    exitCode = ToolRunner.run(conf, new PreprocessPlusForForClique(), args);
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	System.exit(exitCode);
    }

}
