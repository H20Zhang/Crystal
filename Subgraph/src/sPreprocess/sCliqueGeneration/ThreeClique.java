package sPreprocess.sCliqueGeneration;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.TwoNodeKey;


/**
 * Pattern name: Triangle <br>
 * Pattern: (0,1),(1,2),(2,0)
 * 
 * @author zhanghao
 */
public class ThreeClique extends Configured implements Tool {

    public static class NodeIteratorFAMapper extends Mapper<IntWritable, TwoNodeKey, OneNodeKey, OneNodeKey> {
	private final OneNodeKey aKey = new OneNodeKey();
	private final OneNodeKey aValue = new OneNodeKey();

	@Override
	protected void map(IntWritable key, TwoNodeKey value, Context context)
		throws IOException, InterruptedException {
	    if (value.getNode1Degree() < value.getNode2Degree()) {
		aKey.setNode(value.getNode1());
		aKey.setDegree(value.getNode1Degree());
		aValue.setNode(value.getNode2());
		aValue.setDegree(value.getNode2Degree());
		context.write(aKey, aValue);
	    } else {
		aKey.setNode(value.getNode2());
		aKey.setDegree(value.getNode2Degree());
		aValue.setNode(value.getNode1());
		aValue.setDegree(value.getNode1Degree());
		context.write(aKey, aValue);
	    }
	}
    }

    public static class NodeIteratorFAReducer extends Reducer<OneNodeKey, OneNodeKey, TwoNodeKey, OneNodeKey> {

	private BloomFilter[] aBloomFilter;
	private final Key aKey = new Key();
	private final ArrayList<OneNodeKey> aVector = new ArrayList<OneNodeKey>(8192);
	private Integer p = new Integer(1);
	private Integer offset = new Integer(0);
	private byte byteArray[] = new byte[8];

	private final TwoNodeKey outputKey = new TwoNodeKey();
	private final TwoNodeKey outputValue = new TwoNodeKey();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Integer NIBFp = 1;
	    Integer NIBFoffset = 0;

	    p = NIBFp;
	    offset = NIBFoffset;

	    Path[] allPaths = org.apache.hadoop.mapreduce.filecache.DistributedCache.getLocalCacheFiles(context.getConfiguration());

	    aBloomFilter = new BloomFilter[allPaths.length];
	    for (int i = 0; i < allPaths.length; ++i) {
		aBloomFilter[i] = new BloomFilter();
	    }

	    for (int i = 0; i < allPaths.length; ++i) {

		if (i % NIBFp == NIBFoffset) {
		    FileInputStream in = new FileInputStream(new File(allPaths[i].toString()));
		    DataInputStream input = new DataInputStream(in);
		    aBloomFilter[i].readFields(input);
		}
	    }
	}

	public void getBytes(TwoNodeKey edge) {
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
	protected void reduce(OneNodeKey key, Iterable<OneNodeKey> values, Context context)
		throws IOException, InterruptedException {

	    for (OneNodeKey aInt : values) {
		aVector.add(new OneNodeKey(aInt));
	    }

	    for (int i = 0; i < aVector.size(); i++) {
		OneNodeKey a = aVector.get(i);
		for (int j = i + 1; j < aVector.size(); j++) {
		    OneNodeKey b = aVector.get(j);
		    int leading = a.getNode();
		    if (a.getNode() < b.getNode()) {
			outputKey.set(a, b);
		    } else {
			outputKey.set(b, a);
			leading = b.getNode();
		    }

		    getBytes(outputKey);
		    aKey.set(byteArray, 1);

		    if ((leading % 240) % p == offset) {
			context.getCounter("count", "total").increment(1);
			if (aBloomFilter[leading % 240].membershipTest(aKey)) {
			    context.getCounter("count", "tri").increment(1);
			    context.write(outputKey, key);
			    continue;
			}
		    }
		}
	    }
	    aVector.clear();
	}
    }

    public static class NodeIteratorFBMapper extends Mapper<TwoNodeKey, OneNodeKey, TwoNodeKey, OneNodeKey> {
	@Override
	protected void map(TwoNodeKey key, OneNodeKey value, Context context) throws IOException, InterruptedException {
	    context.write(key, value);
	}
    }

    public static class NodeIteratorFBReducer extends Reducer<TwoNodeKey, OneNodeKey, TwoNodeKey, OneNodeKey> {

	private final List<OneNodeKey> aVector = new ArrayList<OneNodeKey>(20480);

	private Boolean isOutput = true;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    isOutput = context.getConfiguration().getBoolean("test.isOutput", true);
	}

	@Override
	protected void reduce(TwoNodeKey key, Iterable<OneNodeKey> values, Context context)
		throws IOException, InterruptedException {
	    boolean containZero = false;
	    for (OneNodeKey aInt : values) {
		if (aInt.getNode() == -1) {
		    containZero = true;
		    continue;
		}
		aVector.add(new OneNodeKey(aInt));
	    }

	    if (containZero == true && isOutput == true) {
		context.getCounter("count", "tri").increment(aVector.size());
		for (OneNodeKey integer : aVector) {
		    context.write(key, integer);
		}
	    }
	    aVector.clear();
	}
    }

    public static class NodeIteratorFCMapper extends Mapper<TwoNodeKey, OneNodeKey, TwoNodeKey, OneNodeKey> {

	private final TwoNodeKey outputKey = new TwoNodeKey();
	private final OneNodeKey first = new OneNodeKey();
	private final OneNodeKey second = new OneNodeKey();
	private final OneNodeKey third = new OneNodeKey();

	@Override
	protected void map(TwoNodeKey key, OneNodeKey value, Context context) throws IOException, InterruptedException {
	    first.set(key.getNode1(), key.getNode1Degree());
	    second.set(key.getNode2(), key.getNode2Degree());
	    third.set(value.getNode(), value.getDegree());

	    // 1 2|3

	    if (first.getNode() < second.getNode()) {
		outputKey.set(first, second);
		context.write(outputKey, third);
	    } else {
		outputKey.set(second, first);
		context.write(outputKey, third);
	    }

	    // 1 3|2
	    if (first.getNode() < third.getNode()) {
		outputKey.set(first, third);
		context.write(outputKey, second);
	    } else {
		outputKey.set(third, first);
		context.write(outputKey, second);
	    }

	    // 2 3|1
	    if (second.getNode() < third.getNode()) {
		outputKey.set(second, third);
		context.write(outputKey, first);
	    } else {
		outputKey.set(third, second);
		context.write(outputKey, first);
	    }
	}
    }

    public static class NodeIteratorFCReducer extends Reducer<TwoNodeKey, OneNodeKey, TwoNodeKey, IntDegreeArray> {
	private final List<OneNodeKey> list = new ArrayList<OneNodeKey>();

	@Override
	protected void reduce(TwoNodeKey key, Iterable<OneNodeKey> values, Context context)
		throws IOException, InterruptedException {
	    for (OneNodeKey oneNodeKey : values) {
		list.add(new OneNodeKey(oneNodeKey));
	    }

	    IntDegreeArray outputValue = new IntDegreeArray(list.size());

	    for (int i = 0; i < list.size(); i++) {
		outputValue.addNodeAndDegree(i, list.get(i).getNode(), list.get(i).getDegree());
	    }

	    context.write(key, outputValue);
	    list.clear();
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	Job job = new Job(conf);
	Integer NIBFoffset = new Integer(conf.get("NIBF.offset"));

	job.setJarByClass(getClass());
	job.setJobName("NFA");

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + NIBFoffset.toString()));

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

//	FileOutputFormat.setCompressOutput(job, true);
//	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
//	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

	job.setMapperClass(NodeIteratorFAMapper.class);

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	boolean success = job.waitForCompletion(true);

	return 0;
    }
}
