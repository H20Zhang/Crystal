package sPreprocess.sCliqueGeneration;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.ThreeNodeKey;
import sData.TwoNodeKey;


/**
 * 
 * This is the main class for generating Triangle, Four Clique, Five Clique and preprocessing them
 * for latter subgraph matching.
 * 
 * @author zhanghao
 *
 */
public class CliqueGeneration extends Configured implements Tool {

    private static Logger log = Logger.getLogger(CliqueGeneration.class);

    @Override
    public int run(String[] args) throws Exception {

	String input = args[0];
	String plus = args[1];
	String Output1 = args[2];
	String plus2 = args[3];
	String Output2 = args[4];
	String plus3 = args[5];
	String Output3 = args[6];
	String bloom = args[7];

	// it determine which pattern is what we want thus the running time when
	// not outputting result
	Integer outputNumber = new Integer(args[8]);

	if (outputNumber == 1) {
	    long startTime = System.currentTimeMillis();
	    generateTriangle(input, plus, Output1, bloom, false);
	    long endTime = System.currentTimeMillis();
	    log.info("triangle Time elapsed: " + (endTime - startTime) / 1000 + "s");
	} else if (outputNumber == 2) {
	    generateTriangle(input, plus, Output1, bloom, true);

	    long startTime = System.currentTimeMillis();

//	    generateFourClique(Output1, plus2, Output2, bloom, false);

	    long endTime = System.currentTimeMillis();
	    log.info("FourClique Time elapsed: " + (endTime - startTime) / 1000 + "s");
	} else if (outputNumber == 3) {
	    generateTriangle(input, plus, Output1, bloom, true);
	    generateFourClique(Output1, plus2, Output2, bloom, true);

	    long startTime = System.currentTimeMillis();
	    generateFiveClique(Output2, plus3, Output3, bloom, false);
	    long endTime = System.currentTimeMillis();
	    log.info("FiveClique Time elapsed: " + (endTime - startTime) / 1000 + "s");
	} else if (outputNumber == 4) {
	    generateTriangle(input, plus, Output1, bloom, true);
	    generateFourClique(Output1, plus2, Output2, bloom, true);
	}

	return 0;
    }

    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf();
	conf.set("mapreduce.reduce.memory.mb", "4000");
	conf.set("mapreduce.reduce.java.opts", "-Xmx3500M");
	// conf.set("mapred.job.tracker", "local"); conf.set("fs.defaultFS",
	// "file:///");
	ToolRunner.run(conf, new CliqueGeneration(), args);
    }

    public void generateTriangle(String Input, String edges, String Output, String Bloom, Boolean isOutput)
	    throws Exception {

	String aString = Bloom;

	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate1"), true);
	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate2"), true);
	FileSystem.get(getConf()).delete(new Path(Output), true);

	for (int i = 0; i < 240; ++i) {
	    String iString = String.valueOf(i);
	    // DistributedCache.addLocalFiles(getConf(), aString + iString);
	    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(aString + iString), getConf());
	}

	// job1
	Job job = new Job(getConf(), "triangleGeneration");
	job.setJarByClass(getClass());

	FileInputFormat.setInputPaths(job, new Path(Input));
	FileOutputFormat.setOutputPath(job, new Path(Input + "." + "Intermidiate1"));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setMapperClass(ThreeClique.NodeIteratorFAMapper.class);
	job.setReducerClass(ThreeClique.NodeIteratorFAReducer.class);

	job.setMapOutputKeyClass(OneNodeKey.class);
	job.setMapOutputValueClass(OneNodeKey.class);
	job.setSortComparatorClass(OneNodeKey.OneNodeKeyComparator.class);

	job.setOutputKeyClass(TwoNodeKey.class);
	job.setOutputValueClass(OneNodeKey.class);

//	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

	boolean success = job.waitForCompletion(true);

	// job2
	Configuration configuration = getConf();
	configuration.setBoolean("test.isOutput", isOutput);
	Job job2 = new Job(configuration, "triangleGeneration");

	job2.setJarByClass(getClass());

	FileInputFormat.addInputPath(job2, new Path(Input + "." + "Intermidiate1"));
	FileInputFormat.addInputPath(job2, new Path(edges));
	FileOutputFormat.setOutputPath(job2, new Path(Input + "." + "Intermidiate2"));

	job2.setInputFormatClass(SequenceFileInputFormat.class);
	job2.setOutputFormatClass(SequenceFileOutputFormat.class);

	job2.setMapperClass(ThreeClique.NodeIteratorFBMapper.class);
	job2.setReducerClass(ThreeClique.NodeIteratorFBReducer.class);

	job2.setMapOutputKeyClass(TwoNodeKey.class);
	job2.setMapOutputValueClass(OneNodeKey.class);
	job2.setSortComparatorClass(TwoNodeKey.TwoNodeKeyComparator.class);

	job2.setOutputKeyClass(TwoNodeKey.class);
	job2.setOutputValueClass(OneNodeKey.class);

	boolean success2 = job2.waitForCompletion(true);

//	SequenceFileOutputFormat.setOutputCompressionType(job2, CompressionType.BLOCK);
//	FileOutputFormat.setOutputCompressorClass(job2, SnappyCodec.class);

	// job3

	if (isOutput == true) {
	    Job job3 = new Job(getConf(), "triangleGeneration");
	    job3.setJarByClass(getClass());

	    FileInputFormat.addInputPath(job3, new Path(Input + "." + "Intermidiate2"));
	    FileOutputFormat.setOutputPath(job3, new Path(Output));

	    job3.setInputFormatClass(SequenceFileInputFormat.class);
	    job3.setOutputFormatClass(SequenceFileOutputFormat.class);

	    job3.setMapperClass(ThreeClique.NodeIteratorFCMapper.class);
	    job3.setReducerClass(ThreeClique.NodeIteratorFCReducer.class);

	    job3.setMapOutputKeyClass(TwoNodeKey.class);
	    job3.setMapOutputValueClass(OneNodeKey.class);
	    job3.setSortComparatorClass(TwoNodeKey.TwoNodeKeyComparator.class);

	    job3.setOutputKeyClass(TwoNodeKey.class);
	    job3.setOutputValueClass(IntDegreeArray.class);

//	    SequenceFileOutputFormat.setOutputCompressionType(job3, CompressionType.BLOCK);
//	    FileOutputFormat.setOutputCompressorClass(job3, SnappyCodec.class);

	    boolean success3 = job3.waitForCompletion(true);
	}

	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate2"), true);
	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate1"), true);
    }

    public void generateFourClique(String Input, String edges, String Output, String Bloom, Boolean isOutput)
	    throws Exception {
	String aString = Bloom;

	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate1"), true);
	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate2"), true);
	FileSystem.get(getConf()).delete(new Path(Output), true);

	for (int i = 0; i < 240; ++i) {
	    String iString = String.valueOf(i);
	    // DistributedCache.addLocalFiles(getConf(), aString + iString);
	    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(aString + iString), getConf());
	}

	// job1
	Job job = new Job(getConf(), "FourClique");
	job.setJarByClass(getClass());

	FileInputFormat.setInputPaths(job, new Path(Input));
	FileOutputFormat.setOutputPath(job, new Path(Input + "." + "Intermidiate1"));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setMapperClass(FourClique.FourCliqueAMapper.class);
	job.setReducerClass(FourClique.FourCliqueFAReducer.class);

	job.setMapOutputKeyClass(TwoNodeKey.class);
	job.setMapOutputValueClass(OneNodeKey.class);
	job.setSortComparatorClass(TwoNodeKey.TwoNodeKeyComparator.class);

	job.setOutputKeyClass(TwoNodeKey.class);
	job.setOutputValueClass(TwoNodeKey.class);

//	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

	boolean success = job.waitForCompletion(true);

	// job2
	Configuration configuration = getConf();
	configuration.setBoolean("test.isOutput", isOutput);
	Job job2 = new Job(configuration, "triangleGeneration");
	job2.setJarByClass(getClass());

	FileInputFormat.addInputPath(job2, new Path(Input + "." + "Intermidiate1"));
	FileInputFormat.addInputPath(job2, new Path(edges));
	FileOutputFormat.setOutputPath(job2, new Path(Input + "." + "Intermidiate2"));

	// FileOutputFormat.setCompressOutput(job2, false);

	job2.setInputFormatClass(SequenceFileInputFormat.class);
	job2.setOutputFormatClass(SequenceFileOutputFormat.class);

	job2.setMapperClass(FourClique.FourCliqueFBMapper.class);
	job2.setReducerClass(FourClique.FourCliqueFBReducer.class);

	job2.setMapOutputKeyClass(TwoNodeKey.class);
	job2.setMapOutputValueClass(TwoNodeKey.class);
	job2.setSortComparatorClass(TwoNodeKey.TwoNodeKeyComparator.class);

	job2.setOutputKeyClass(TwoNodeKey.class);
	job2.setOutputValueClass(TwoNodeKey.class);

//	SequenceFileOutputFormat.setOutputCompressionType(job2, CompressionType.BLOCK);
//	FileOutputFormat.setOutputCompressorClass(job2, SnappyCodec.class);

	boolean success2 = job2.waitForCompletion(true);

	// job3

	if (isOutput == true) {
	    Job job3 = new Job(getConf(), "triangleGeneration");
	    job3.setJarByClass(getClass());

	    FileInputFormat.addInputPath(job3, new Path(Input + "." + "Intermidiate2"));
	    FileOutputFormat.setOutputPath(job3, new Path(Output));

	    job3.setInputFormatClass(SequenceFileInputFormat.class);
	    job3.setOutputFormatClass(SequenceFileOutputFormat.class);

	    job3.setMapperClass(FourClique.FourCliqueFCMapper.class);
	    job3.setReducerClass(FourClique.FourCliqueFCReducer.class);

	    job3.setMapOutputKeyClass(ThreeNodeKey.class);
	    job3.setMapOutputValueClass(OneNodeKey.class);
	    job3.setSortComparatorClass(ThreeNodeKey.ThreeNodeKeyComparator.class);

	    job3.setOutputKeyClass(ThreeNodeKey.class);
	    job3.setOutputValueClass(IntDegreeArray.class);

//	    SequenceFileOutputFormat.setOutputCompressionType(job3, CompressionType.BLOCK);
//	    FileOutputFormat.setOutputCompressorClass(job3, SnappyCodec.class);

	    boolean success3 = job3.waitForCompletion(true);
	}

	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate2"), true);
	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate1"), true);
    }

    public void generateFiveClique(String Input, String edges, String Output, String Bloom, Boolean isOutput)
	    throws Exception {
	String aString = Bloom;

	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate1"), true);
	FileSystem.get(getConf()).delete(new Path(Input + "." + "Intermidiate2"), true);
	FileSystem.get(getConf()).delete(new Path(Output), true);

	for (int i = 0; i < 240; ++i) {
	    String iString = String.valueOf(i);
	    // DistributedCache.addLocalFiles(getConf(), aString + iString);
	    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(aString + iString), getConf());
	}

	// job1
	Job job = new Job(getConf(), "FiveClique1");
	job.setJarByClass(getClass());

	FileInputFormat.setInputPaths(job, new Path(Input));
	FileOutputFormat.setOutputPath(job, new Path(Input + "." + "Intermidiate1"));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setMapperClass(FiveClique.FiveCliqueAMapper.class);
	job.setReducerClass(FiveClique.FiveCliqueFAReducer.class);

	job.setMapOutputKeyClass(ThreeNodeKey.class);
	job.setMapOutputValueClass(OneNodeKey.class);
	job.setSortComparatorClass(ThreeNodeKey.ThreeNodeKeyComparator.class);

	job.setOutputKeyClass(TwoNodeKey.class);
	job.setOutputValueClass(ThreeNodeKey.class);

	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

	boolean success = job.waitForCompletion(true);

	// job2
	Configuration configuration = getConf();
	configuration.setBoolean("test.isOutput", isOutput);
	Job job2 = new Job(configuration, "FiveClique2");
	job2.setJarByClass(getClass());

	FileInputFormat.addInputPath(job2, new Path(Input + "." + "Intermidiate1"));
	FileInputFormat.addInputPath(job2, new Path(edges));
	FileOutputFormat.setOutputPath(job2, new Path(Input + "." + "Intermidiate2"));

	// FileOutputFormat.setCompressOutput(job2, false);

	job2.setInputFormatClass(SequenceFileInputFormat.class);
	job2.setOutputFormatClass(SequenceFileOutputFormat.class);

	job2.setMapperClass(FiveClique.FiveCliqueFBMapper.class);
	job2.setReducerClass(FiveClique.FiveCliqueFBReducer.class);

	job2.setMapOutputKeyClass(TwoNodeKey.class);
	job2.setMapOutputValueClass(ThreeNodeKey.class);
	job2.setSortComparatorClass(ThreeNodeKey.ThreeNodeKeyComparator.class);

	job2.setOutputKeyClass(TwoNodeKey.class);
	job2.setOutputValueClass(ThreeNodeKey.class);

	SequenceFileOutputFormat.setOutputCompressionType(job2, CompressionType.BLOCK);
	FileOutputFormat.setOutputCompressorClass(job2, SnappyCodec.class);

	boolean success2 = job2.waitForCompletion(true);
    }

}
