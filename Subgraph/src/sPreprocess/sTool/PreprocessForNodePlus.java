package sPreprocess.sTool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import sData.OneNodeKey;
import sData.TwoNodeKey;

public class PreprocessForNodePlus extends Configured implements Tool {

    public static class PreprocessBMapper extends Mapper<IntWritable, TwoNodeKey, TwoNodeKey, OneNodeKey> {
	private final TwoNodeKey aKey = new TwoNodeKey();
	private final OneNodeKey aValue = new OneNodeKey();

	@Override
	protected void map(IntWritable key, TwoNodeKey value, Context context)
		throws IOException, InterruptedException {
	    aValue.setNode(-1);
	    context.write(value, aValue);
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	conf.set("mapreduce.map.memory.mb", "4000");
	conf.set("mapreduce.map.java.opts", "-Xmx3500M");
	Job job = new Job(conf);

	job.setJarByClass(PreprocessManager.class);
	job.setJobName("Preprocess");

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setNumReduceTasks(0);

	job.setMapperClass(PreprocessBMapper.class);

	job.setMapOutputKeyClass(TwoNodeKey.class);
	job.setMapOutputValueClass(OneNodeKey.class);

	job.setOutputKeyClass(TwoNodeKey.class);
	job.setOutputValueClass(OneNodeKey.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }

}
