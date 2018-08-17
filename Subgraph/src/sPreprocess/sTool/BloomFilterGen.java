package sPreprocess.sTool;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import sData.EdgeWritable;

public class BloomFilterGen extends Configured implements Tool {

    public static class BloomFilterGenMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

	private final IntWritable aKey = new IntWritable();
	private final IntWritable aValue = new IntWritable();

	@Override
	protected void map(IntWritable key, IntWritable value, Context context)
		throws IOException, InterruptedException {
	    int a = key.get();
	    int b = value.get();
	    if (a < b) {
		aKey.set(a);
		aValue.set(b);
	    } else {
		aKey.set(b);
		aValue.set(a);
	    }
	    context.write(aKey, aValue);
	}
    }

    public static class BloomFilterGenReducer extends Reducer<IntWritable, IntWritable, IntWritable, BloomFilter> {
	private BloomFilter aBloomFilter;
	private final EdgeWritable aEdge = new EdgeWritable();
	private byte byteArray[] = new byte[8];
	private final Key aBKey = new Key();

	@Override
	protected void setup(Reducer<IntWritable, IntWritable, IntWritable, BloomFilter>.Context context)
		throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    Long bloom_size_raw = new Long(conf.get("bloom_size"));
	    Integer bit_per_record = new Integer(conf.get("bit_per_record"));
	    Integer hash_number = new Integer(conf.get("hash_number"));
	    Integer bloom_size = (int) (bloom_size_raw / 240 * bit_per_record);
	    aBloomFilter = new BloomFilter(bloom_size, hash_number, Hash.MURMUR_HASH);
	}

	public void getBytes(EdgeWritable edge) {

	    /*
	     * int temp1 = (node1 * node2) / 2; int temp2 = node2 - node1;
	     * 
	     * byteArray[7] = (byte) ((temp1 >> 24) & 0xFF); byteArray[1] =
	     * (byte) ((temp1 >> 16) & 0xFF); byteArray[6] = (byte) ((temp1 >>
	     * 8) & 0xFF); byteArray[4] = (byte) ((temp1) & 0xFF); byteArray[3]
	     * = (byte) ((temp2 >> 24) & 0xFF); byteArray[2] = (byte) ((temp2 >>
	     * 16) & 0xFF); byteArray[5] = (byte) ((temp2 >> 8) & 0xFF);
	     * byteArray[0] = (byte) ((temp2) & 0xFF);
	     * 
	     * byteArray[1] = (byte) (byteArray[1] & byteArray[3]); byteArray[2]
	     * = (byte) (byteArray[2] & byteArray[7]);
	     */
	    int node1 = edge.getNode1();
	    int node2 = edge.getNode2();

	    byteArray[7] = (byte) ((node1 >> 24) & 0xFF);
	    byteArray[1] = (byte) ((node1 >> 16) & 0xFF);
	    byteArray[6] = (byte) ((node1 >> 8) & 0xFF);
	    byteArray[4] = (byte) ((node1) & 0xFF);
	    byteArray[3] = (byte) ((node2 >> 24) & 0xFF);
	    byteArray[2] = (byte) ((node2 >> 16) & 0xFF);
	    byteArray[5] = (byte) ((node2 >> 8) & 0xFF);
	    byteArray[0] = (byte) ((node2) & 0xFF);

	}

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    for (IntWritable value : values) {
		// context.getCounter("total",
		// conf.get("mapred.task.partition")).increment(1);
		int node1 = key.get();
		int node2 = value.get();

		aEdge.setNode1(node1);
		aEdge.setNode2(node2);
		getBytes(aEdge);
		aBKey.set(byteArray, 1);
		aBloomFilter.add(aBKey);
	    }

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    Path file = new Path(
		    conf.get("mapred.output.dir") + "/bloom_file" + "/bloom" + conf.get("mapred.task.partition"));
	    FSDataOutputStream out = file.getFileSystem(conf).create(file);
	    DataOutputStream dataout = new DataOutputStream(out);
	    aBloomFilter.write(dataout);
	    dataout.close();
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	Job job = new Job(conf);

	job.setJarByClass(BloomFilterGeneratorManager.class);
	job.setJobName("Bloom Filter");
	job.setNumReduceTasks(240);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	// job.setOutputFormatClass(SequenceFileOutputFormat.class);
	/*
	 * job.setOutputFormatClass(SequenceFileOutputFormat.class);
	 * FileOutputFormat.setCompressOutput(job, true);
	 * FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
	 * SequenceFileOutputFormat.setOutputCompressionType(job,
	 * CompressionType.BLOCK);
	 */

	job.setMapperClass(BloomFilterGenMapper.class);
	job.setReducerClass(BloomFilterGenReducer.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(BloomFilter.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }

}
