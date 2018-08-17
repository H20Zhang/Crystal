package sPreprocess.sTool;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import sData.TwoNodeKey;
import sData.EdgeWritable;

public class DegreeCounterA extends Configured implements Tool {

    public static class DegreeCounterAMapper extends Mapper<IntWritable, IntWritable, IntWritable, EdgeWritable> {

	EdgeWritable aEdge = new EdgeWritable();
	IntWritable aKey = new IntWritable();

	@Override
	protected void map(IntWritable key, IntWritable value, Context context)
		throws IOException, InterruptedException {

	    int node1 = key.get();
	    int node2 = value.get();

	    aEdge.setNode1(node1);
	    aEdge.setNode2(node2);

	    aKey.set(node1);
	    context.write(aKey, aEdge);

	    aKey.set(node2);
	    context.write(aKey, aEdge);
	}
    }

    public static class DegreeCounterAReducer extends Reducer<IntWritable, EdgeWritable, EdgeWritable, TwoNodeKey> {

	private final TwoNodeKey aEdgeDgreeWritable = new TwoNodeKey();

	public void caclDgreeWritable(int aCounter, EdgeWritable aEdgeWritable, int aNode) {
	    aEdgeDgreeWritable.setNode1(aEdgeWritable.getNode1());
	    aEdgeDgreeWritable.setNode2(aEdgeWritable.getNode2());

	    if (aNode == aEdgeWritable.getNode1()) {
		aEdgeDgreeWritable.setNode1Degree(aCounter);
		aEdgeDgreeWritable.setNode2Degree(-1);
	    } else {
		aEdgeDgreeWritable.setNode1Degree(-1);
		aEdgeDgreeWritable.setNode2Degree(aCounter);
	    }
	}

	private final LinkedList<EdgeWritable> aList = new LinkedList<EdgeWritable>();

	@Override
	protected void reduce(IntWritable key, Iterable<EdgeWritable> values, Context context)
		throws IOException, InterruptedException {

	    int counter = 0;
	    for (EdgeWritable aEdgeWritable : values) {
		EdgeWritable edgeToAdd = new EdgeWritable(aEdgeWritable.getNode1(), aEdgeWritable.getNode2());
		aList.add(edgeToAdd);
		++counter;
	    }

	    for (EdgeWritable aEdgeWritable : aList) {
		caclDgreeWritable(counter, aEdgeWritable, key.get());
		context.write(aEdgeWritable, aEdgeDgreeWritable);
	    }

	    aList.clear();
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	Job job = new Job(conf);
	job.setNumReduceTasks(240);

	job.setJarByClass(CountDegree.class);
	job.setJobName("DegreeCountA");

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
//	FileOutputFormat.setCompressOutput(job, true);
//	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
//	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

	job.setMapperClass(DegreeCounterAMapper.class);
	job.setReducerClass(DegreeCounterAReducer.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(EdgeWritable.class);

	job.setOutputKeyClass(EdgeWritable.class);
	job.setOutputValueClass(TwoNodeKey.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }

}
