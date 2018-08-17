package sPreprocess.sTool;

import java.io.IOException;

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

public class DegreeCounterB extends Configured implements Tool {

    public static class DegreeCounterBMapper extends Mapper<EdgeWritable, TwoNodeKey, EdgeWritable, TwoNodeKey> {

	@Override
	protected void map(EdgeWritable key, TwoNodeKey value, Context context)
		throws IOException, InterruptedException {
	    context.write(key, value);
	}
    }

    public static class DegreeCounterBReducer extends Reducer<EdgeWritable, TwoNodeKey, IntWritable, TwoNodeKey> {

	private TwoNodeKey aEdgeDegree = new TwoNodeKey();
	private IntWritable aKey = new IntWritable(0);

	@Override
	protected void reduce(EdgeWritable key, Iterable<TwoNodeKey> values, Context context)
		throws IOException, InterruptedException {

	    aEdgeDegree.setNode1(key.getNode1());
	    aEdgeDegree.setNode2(key.getNode2());
	    for (TwoNodeKey edgeDgreeWritable : values) {

		if (edgeDgreeWritable.getNode1Degree() != -1) {
		    aEdgeDegree.setNode1Degree(edgeDgreeWritable.getNode1Degree());
		} else {
		    aEdgeDegree.setNode2Degree(edgeDgreeWritable.getNode2Degree());
		}
	    }
	    context.write(aKey, aEdgeDegree);
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	Job job = new Job(conf);
	job.setNumReduceTasks(240);

	job.setJarByClass(CountDegree.class);
	job.setJobName("DegreeCountB");

	FileInputFormat.setInputPaths(job, new Path(args[1]));
	FileOutputFormat.setOutputPath(job, new Path(args[2]));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
//	FileOutputFormat.setCompressOutput(job, true);
//	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
//	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

	job.setMapperClass(DegreeCounterBMapper.class);
	job.setReducerClass(DegreeCounterBReducer.class);

	job.setMapOutputKeyClass(EdgeWritable.class);
	job.setMapOutputValueClass(TwoNodeKey.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(TwoNodeKey.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }
}
