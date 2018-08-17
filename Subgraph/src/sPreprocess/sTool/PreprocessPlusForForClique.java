package sPreprocess.sTool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import sData.TwoNodeKey;

public class PreprocessPlusForForClique extends Configured implements Tool {

    public static class PreprocessFourCliqueBMapper extends Mapper<IntWritable, TwoNodeKey, TwoNodeKey, TwoNodeKey> {
	private final TwoNodeKey aKey = new TwoNodeKey();
	private final TwoNodeKey aValue = new TwoNodeKey();

	@Override
	protected void map(IntWritable key, TwoNodeKey value, Context context)
		throws IOException, InterruptedException {
	    aValue.setNode1(-1);
	    context.write(value, aValue);
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	Job job = new Job(conf);

	job.setJarByClass(PreprocessManager.class);
	job.setJobName("Preprocess");

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	job.setNumReduceTasks(0);

	job.setMapperClass(PreprocessFourCliqueBMapper.class);

	job.setMapOutputKeyClass(TwoNodeKey.class);
	job.setMapOutputValueClass(TwoNodeKey.class);

	job.setOutputKeyClass(TwoNodeKey.class);
	job.setOutputValueClass(TwoNodeKey.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }

}
