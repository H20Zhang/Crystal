package sAlg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sData.IntDegreeArray;
import sData.TwoNodeKey;



/**
 * 
 * 
 * <h1>
 * Output Chordal Square
 * </h1>
 * <ul>
 * 	<li>Pattern name: Chordal Square </li>
 * 	<li>Pattern: (0,1),(0,2),(0,3),(1,2),(2,3)  </li>
 * 	<li>Core: (0,2)</li>
 * 	<li>Non Core Node: [1,3]</li>
 * </ul>
 * 
 * 
 * @author zhanghao
 */
public class ChordalSquareO extends Configured implements Tool {

    private static Logger log = Logger.getLogger(ChordalSquareO.class);

    public static class ChordalSquareMapper extends Mapper<TwoNodeKey, IntDegreeArray, TwoNodeKey, IntDegreeArray> {
	public boolean isOutput = false;

	@Override
	protected void setup(Mapper<TwoNodeKey, IntDegreeArray, TwoNodeKey, IntDegreeArray>.Context context)
		throws IOException, InterruptedException {
	    isOutput = context.getConfiguration().getBoolean("test.isOutput", false);
	}

	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {

	    if (isOutput == true) {
		context.write(key, value);
	    } else {
		for (int i = 0; i < value.size; ++i) {
		    for (int j = i; j < value.size; ++j) {
			int node1 = value.getNode(i);
			int node2 = value.getNode(j);
		    }
		}
		context.getCounter("test", "core").increment(1);
		context.getCounter("test", "list_size").increment(value.size);
		context.getCounter("test", "pattern_size").increment(value.size * (value.size - 1) / 2);
	    }
	}
    }

    @Override
    public int run(String[] args) throws Exception {

	Configuration conf = getConf();

	int memory_size = conf.getInt("test.memory", 4000);
	String memory_opts = "-Xmx" + memory_size + "M";

	conf.setInt("mapreduce.map.memory.mb", memory_size);
	conf.set("mapreduce.map.java.opts", memory_opts);
	conf.setInt("mapreduce.reduce.memory.mb", memory_size);
	conf.set("mapreduce.reduce.java.opts", memory_opts);

	Job job = new Job(conf);

	job.setJarByClass(getClass());
	job.setJobName("ChordalSquareO");

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

//	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
	
	job.setOutputKeyClass(TwoNodeKey.class);
	job.setOutputValueClass(IntDegreeArray.class);

	job.setMapOutputKeyClass(TwoNodeKey.class);
	job.setMapOutputValueClass(IntDegreeArray.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setNumReduceTasks(0);

	job.setMapperClass(ChordalSquareMapper.class);

	boolean success = job.waitForCompletion(true);

	return 0;
    }

    public static void main(String[] args) throws Exception {

	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();
	JobConf conf = new JobConf();
	ToolRunner.run(conf, new ChordalSquareO(), args);
	endTime = System.currentTimeMillis();
	log.info("[chordal square] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
