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

public class PreprocessForClue extends Configured implements Tool {

    public static class PreprocessForClueMapper extends Mapper<LongWritable, Text, EdgeWritable, IntWritable> {
	private final EdgeWritable aKey = new EdgeWritable();
	private final IntWritable aValue = new IntWritable();
	private Integer Count = new Integer(0);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String[] aStrings = value.toString().split(" ");

	    try {
		for (int i = 0; i < aStrings.length; i++) {
		    Integer node2 = Integer.parseInt(aStrings[i]);
		    Integer node1 = Count;
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
		}
	    } catch (Exception e) {
	    }
	    Count++;
	}
    }

    public static class PreprocessForClueReducer extends Reducer<EdgeWritable, IntWritable, IntWritable, IntWritable> {

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

	job.setJarByClass(PreprocessManager.class);
	job.setJobName("Preprocess");

	String aString = conf.get("mapreduce.reduce.memory.mb");
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
	job.setMapperClass(PreprocessForClueMapper.class);
	job.setReducerClass(PreprocessForClueReducer.class);

	job.setMapOutputKeyClass(EdgeWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }
}
