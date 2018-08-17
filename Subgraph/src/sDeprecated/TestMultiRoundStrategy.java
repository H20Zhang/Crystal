package sDeprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.TwoNodeKey;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

public class TestMultiRoundStrategy extends Configured implements Tool {

    public static String appendNumberToLengthThree(Integer i) {
	if (i > 0) {
	    if (i < 10) {
		return "00" + i;
	    } else if (i < 100) {
		return "0" + i;
	    } else {
		return i.toString();
	    }
	} else {
	    return "000";
	}
    }

    public static class TestMultiRoundStrategyMapper
	    extends Mapper<TwoNodeKey, IntDegreeArray, IntWritable, IntWritable> {
	private BloomFilter[] aBloomFilter;
	private final Key aKey = new Key();
	private final ArrayList<OneNodeKey> aVector = new ArrayList<OneNodeKey>(8192);
	private Integer p = new Integer(1);
	private Integer offset = new Integer(0);
	private byte byteArray[] = new byte[8];
	private final ArrayListPool<OneNodeKey> thePool = new ArrayListPool<OneNodeKey>(1000);

	private final TwoNodeKey outputKey = new TwoNodeKey();
	private final TwoNodeKey outputValue = new TwoNodeKey();
	private final HashMap<OneNodeKey, ArrayList<OneNodeKey>> map = new HashMap<OneNodeKey, ArrayList<OneNodeKey>>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	    Configuration conf = context.getConfiguration();
	    Integer NIBFp = conf.getInt("test.p", 1);
	    Integer NIBFoffset = conf.getInt("test.offset", 0);

	    Path[] allPaths = org.apache.hadoop.mapreduce.filecache.DistributedCache.getLocalCacheFiles(context.getConfiguration());

	    p = NIBFp;
	    offset = NIBFoffset;

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);

	    ArrayList<Integer> shuffleList = new ArrayList<Integer>();
	    for (int i = 0; i < 240; ++i) {

		if ((i % p) == offset) {
		    shuffleList.add(i);
		}
	    }

	    String path = conf.get("test.line") + "/part-r-00";
	    Collections.shuffle(shuffleList, new Random(System.currentTimeMillis()));

	    for (int k = 0; k < shuffleList.size(); k++) {
		// FileInputStream in = new FileInputStream(new
		// File(allPaths[k].toString()));
		// DataInputStream input = new DataInputStream(in);
		// FSDataInputStream fsin = new FSDataInputStream(input);
		// SequenceFile.Reader.Option file =
		// SequenceFile.Reader.file(allPaths[k]);
		// SequenceFile.Reader.Option stream =
		// SequenceFile.Reader.stream(fsin);
		// reader = new SequenceFile.Reader(conf, file, stream);

		reader = new SequenceFile.Reader(FileSystem.getLocal(conf), allPaths[k], conf);
		// reader = new SequenceFile.Reader(fs, new
		// Path(path+appendNumberToLengthThree(shuffleList.get(k))),
		// conf);
		OneNodeKey key = (OneNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		IntDegreeArray value = (IntDegreeArray) ReflectionUtils.newInstance(reader.getValueClass(), conf);

		long position = reader.getPosition();
		while (reader.next(key, value)) {
		    String syncSeen = reader.syncSeen() ? "*" : "";
		    // System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
		    // key, value);
		    position = reader.getPosition(); // beginning of next record
		    OneNodeKey theCopiedKey = new OneNodeKey(key);

		    for (int h = 0; h < value.size; ++h) {
			OneNodeKey keyToAddMap = new OneNodeKey();
			keyToAddMap.set(value.getNode(h), value.getDegree(h));
			if (map.containsKey(keyToAddMap)) {
			    map.get(keyToAddMap).add(theCopiedKey);

			} else {
			    ArrayList<OneNodeKey> vi = new ArrayList<OneNodeKey>();
			    vi.add(theCopiedKey);
			    map.put(keyToAddMap, vi);
			}
		    }
		}
		IOUtils.closeStream(reader);
	    }
	}

	private HashMap<OneNodeKey, ArrayList<OneNodeKey>> tp = new HashMap<OneNodeKey, ArrayList<OneNodeKey>>();

	public static class ArrayListPool<E> {
	    private final ArrayList<ArrayList<E>> tempArray = new ArrayList<ArrayList<E>>();
	    private Integer allocatedNumber = 0;

	    public ArrayListPool(int initSize) {
		for (int i = 0; i < initSize; ++i) {
		    tempArray.add(new ArrayList<E>());
		}
	    }

	    public ArrayList<E> borrowObject() {
		if (allocatedNumber < tempArray.size()) {
		    return tempArray.get(allocatedNumber++);
		} else {
		    ArrayList<E> newObject = new ArrayList<E>();
		    return newObject;
		}
	    }

	    public void returnAll() {
		allocatedNumber = 0;
	    }
	}

	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value,
		Mapper<TwoNodeKey, IntDegreeArray, IntWritable, IntWritable>.Context context)
		throws IOException, InterruptedException {

	    for (int j = 0; j < value.size; ++j) {
		OneNodeKey tpKey = new OneNodeKey();
		tpKey.set(value.getNode(j), value.getDegree(j));
		if (map.containsKey(tpKey)) {
		    ArrayList<OneNodeKey> intersectList = map.get(tpKey);

		    for (OneNodeKey node : intersectList) {

			if (tp.containsKey(node)) {
			    tp.get(node).add(tpKey);
			} else {
			    ArrayList<OneNodeKey> aList = new ArrayList<OneNodeKey>();
			    aList.add(tpKey);
			    tp.put(node, aList);
			}
		    }
		}
	    }

	    for (OneNodeKey theKey : tp.keySet()) {
		if (theKey.getNode() != key.getNode1() && theKey.getNode() != key.getNode2()) {
		    ArrayList<OneNodeKey> theValue = tp.get(theKey);
		    for (int i = 0; i < theValue.size(); i++) {
			for (int j = i; j < theValue.size(); j++) {

			}
		    }
		    context.getCounter("test", "graph").increment((theValue.size() * (theValue.size() - 1) / 2));
		}
	    }
	    tp.clear();
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();

	// create seed file
	for (int i = 0; i < 240; ++i) {
	    // DistributedCache.addLocalFiles(getConf(), aString + iString);
	    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(args[1] + "/part-r-00" + appendNumberToLengthThree(i)), getConf());
	}

	conf.set("mapreduce.map.memory.mb", "5000");
	conf.set("mapreduce.map.java.opts", "-Xmx5000M");
	conf.set("test.line", args[1]);

	Job job = new Job(conf);

	job.setJarByClass(getClass());
	job.setJobName("Preprocess");

	job.setInputFormatClass(SequenceFileInputFormat.class);
	// job.setOutputFormatClass(NullOutputFormat.class);

	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.setNumReduceTasks(0);

	job.setMapperClass(TestMultiRoundStrategyMapper.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf();
	// conf.setProfileEnabled(true);
	long milliSeconds = 10000 * 60 * 60; // <default is 600000, likewise can
					     // give any value)
	conf.setLong("mapred.task.timeout", milliSeconds);

	conf.setNumMapTasks(240);
	int p = 1;
	for (int i = 0; i < p; i++) {
	    conf.setInt("test.p", p);
	    conf.setInt("test.offset", i);
	    ToolRunner.run(conf, new TestMultiRoundStrategy(), args);
	}

    }

}
