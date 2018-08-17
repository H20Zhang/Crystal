package sPreprocess.sTool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import sData.EdgeWritable;

public class Preprocess extends Configured implements Tool {
    public static class PreprocessMapper extends Mapper<LongWritable, Text, EdgeWritable, IntWritable> {
	private final EdgeWritable aKey = new EdgeWritable();
	private final IntWritable aValue = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    // String[] aStrings = value.toString().split("\t");
	    String[] aStrings = value.toString().split("\\s");
	    String aString = aStrings[0];
	    try {
		int node1 = Integer.parseInt(aString);
		int node2 = Integer.parseInt(aStrings[1]);
		if (node1 != node2) {
		    if (node1 < node2) {
			aKey.setNode1(node1);
			aKey.setNode2(node2);
			aValue.set(node1);
		    } else {
			aKey.setNode1(node2);
			aKey.setNode2(node1);
			aValue.set(node1);
		    }
		    context.write(aKey, aValue);
		}

	    } catch (Exception e) {
	    }
	}
    }

    public static class PreprocessReducer extends Reducer<EdgeWritable, IntWritable, IntWritable, IntWritable> {

	private final IntWritable aKey = new IntWritable();
	private final IntWritable aValue = new IntWritable();

	@Override
	protected void reduce(EdgeWritable key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
	    aKey.set(key.getNode1());
	    aValue.set(key.getNode2());
	    context.write(aKey, aValue);
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	Job job = new Job(conf);
	job.setNumReduceTasks(240);

	job.setJarByClass(PreprocessManager.class);
	job.setJobName("Preprocess");

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	// job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	/*
	 * job.setOutputFormatClass(SequenceFileOutputFormat.class);
	 * FileOutputFormat.setCompressOutput(job, true);
	 * FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
	 * SequenceFileOutputFormat.setOutputCompressionType(job,
	 * CompressionType.BLOCK);
	 */
	job.setMapperClass(PreprocessMapper.class);
	job.setReducerClass(PreprocessReducer.class);

	job.setMapOutputKeyClass(EdgeWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }
}
