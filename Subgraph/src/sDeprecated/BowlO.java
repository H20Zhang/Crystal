package sDeprecated;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sAlg.ThreeTriangleO;
import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.TwoNodeKey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

public class BowlO extends Configured implements Tool {

    private static Logger log = Logger.getLogger(ThreeTriangleO.class);

    public static class OneNodeKeyIntDegreeArray implements Writable {

	public OneNodeKey key;
	public IntDegreeArray array;

	public OneNodeKeyIntDegreeArray() {
	    key = new OneNodeKey();
	    array = new IntDegreeArray();
	}

	public OneNodeKeyIntDegreeArray(OneNodeKey key, IntDegreeArray array) {
	    this.key = key;
	    this.array = array;
	}

	public void set(OneNodeKey key, IntDegreeArray array) {
	    this.key = key;
	    this.array = array;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
	    key.readFields(in);
	    array.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
	    key.write(out);
	    array.write(out);
	}
    }

	private String createSeedFile(String seedFile, int p) throws IOException {
		Configuration conf = getConf();

		List<String> seedList = new ArrayList();

		FSDataOutputStream seedOut = FileSystem.get(conf).create(new Path(seedFile));

		for (int i = 0; i < p; i++) {
			for (int j = 0; j < p; j++) {
				seedList.add(i + " " + j + "\n");
			}
		}

		Collections.shuffle(seedList);

		System.out.printf("[Info] %d subproblem(s) to process\n", Integer.valueOf(seedList.size()));

		for (String s : seedList) {
			seedOut.writeBytes(s);
		}

		seedOut.close();

		return seedFile;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		FileSystem.get(conf).delete(new Path(args[0] + ".temp"));
		FileSystem.get(conf).delete(new Path(args[0] + ".temp2"));
		FileSystem.get(conf).delete(new Path(args[0] + ".seed"));

		// create seed file

		int p = conf.getInt("test.p", 40);
		String seed = args[0] + ".seed";
		createSeedFile(seed, p);

		int memory_size = conf.getInt("test.memory", 4000);
		int opts_size = (int) (memory_size * 0.8);
		String memory_opts = "-Xmx" + opts_size + "M";

		conf.setInt("mapreduce.map.memory.mb", memory_size);
		conf.set("mapreduce.map.java.opts", memory_opts);
		conf.setInt("mapreduce.reduce.memory.mb", memory_size);
		conf.set("mapreduce.reduce.java.opts", memory_opts);
		conf.set("test.triangleIntermidiate", args[0] + ".triangleIntermidiate/temp");
		conf.set("test.triangleIntermidiate2", args[0] + ".triangleIntermidiate2/temp");
		conf.setInt("test.p", p);

		FileSystem.get(conf).delete(new Path(conf.get("test.triangleIntermidiate")));
		FileSystem.get(conf).delete(new Path(conf.get("test.triangleIntermidiate2")));

		// job1
		Job job1 = new Job(conf);
		job1.setJarByClass(getClass());
		job1.setJobName("Preprocess1");

		job1.setInputFormatClass(SequenceFileInputFormat.class);

		job1.setMapperClass(BowlOMapper1.class);
		job1.setReducerClass(BowlOReducer1.class);

		FileInputFormat.setInputPaths(job1, args[0]);
		FileOutputFormat.setOutputPath(job1, new Path(args[0] + ".temp"));
		FileOutputFormat.setCompressOutput(job1, false);

		job1.setMapOutputKeyClass(BowlParitionKey.class);
		job1.setMapOutputValueClass(OneNodeKeyIntDegreeArray.class);

		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(NullWritable.class);

		boolean success = job1.waitForCompletion(true);

		// job2

		Job job2 = new Job(conf);
		job2.setJarByClass(getClass());
		job2.setJobName("Preprocess2");

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		job2.setMapperClass(BowlOMapper2.class);
		job2.setReducerClass(BowlOReducer2.class);

		FileInputFormat.setInputPaths(job2, args[0]);
		FileOutputFormat.setOutputPath(job2, new Path(args[0] + ".temp2"));

		job2.setMapOutputKeyClass(BowlParitionTwoKey.class);
		job2.setMapOutputValueClass(TwoNodeKey.class);

		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(NullWritable.class);

		success = job2.waitForCompletion(true);
		// job3

		String path = conf.get("test.triangleIntermidiate");

		for (int i = 0; i < p; ++i) {
			org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(path + "-" + i), getConf());
		}

		Job job3 = new Job(conf);

		job3.setJarByClass(getClass());
		job3.setJobName("Preprocess");

		job3.getConfiguration().setInt("mapred.line.input.format.linespermap", p / 5);

		FileInputFormat.setInputPaths(job3, new Path(seed));

		job3.setInputFormatClass(NLineInputFormat.class);
		job3.setOutputFormatClass(NullOutputFormat.class);

		job3.setNumReduceTasks(0);

		job3.setMapperClass(BowlOMapper3.class);

		job3.waitForCompletion(true);

		return 0;
	}

    public static class BowlOMapper1
	    extends Mapper<OneNodeKey, IntDegreeArray, BowlParitionKey, OneNodeKeyIntDegreeArray> {

	private int p = 1;
	private final BowlParitionKey aKey = new BowlParitionKey();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    p = context.getConfiguration().getInt("test.p", 1);
	}

	@Override
	protected void map(OneNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    aKey.setKey(key.getNode(), p);
	    context.write(aKey, new OneNodeKeyIntDegreeArray(key, value));
	}
    }

    public static class BowlOReducer1
	    extends Reducer<BowlParitionKey, OneNodeKeyIntDegreeArray, NullWritable, NullWritable> {
	@Override
	protected void reduce(BowlParitionKey key, Iterable<OneNodeKeyIntDegreeArray> values, Context context)
		throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    String path = conf.get("test.triangleIntermidiate");
	    String outputPath = path + "-" + key.key1;
	    FileSystem fileSystem = null;

	    fileSystem = FileSystem.get(conf);

	    // 定义输出流（SequenceFile）

	    SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, conf, new Path(outputPath),
		    OneNodeKey.class, IntDegreeArray.class);
	    for (OneNodeKeyIntDegreeArray onenodeArray : values) {
		OneNodeKeyIntDegreeArray outputResult = WritableUtils.clone(onenodeArray, context.getConfiguration());
		writer.append(outputResult.key, outputResult.array);
	    }
	    IOUtils.closeStream(writer);
	}
    }

    public static class BowlOMapper2 extends Mapper<OneNodeKey, IntDegreeArray, BowlParitionTwoKey, TwoNodeKey> {

	private Integer p;
	private final BowlParitionTwoKey aKey = new BowlParitionTwoKey();
	private final TwoNodeKey aValue = new TwoNodeKey();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    p = conf.getInt("test.p", 1);
	}

	@Override
	protected void map(OneNodeKey key, IntDegreeArray value, Context context)
		throws IOException, InterruptedException {
	    int size = value.size;
	    for (int i = 0; i < size; ++i) {
		if (key.getNode() < value.getNode(i)) {
		    aValue.setNode1(key.node);
		    aValue.setNode2(value.getNode(i));
		    aValue.setNode1Degree(key.degree);
		    aValue.setNode2Degree(value.getDegree(i));
		    aKey.setKey(aValue.node1, aValue.node2, p);
		    context.write(aKey, aValue);
		}

	    }
	}
    }

    public static class BowlOReducer2 extends Reducer<BowlParitionTwoKey, TwoNodeKey, NullWritable, NullWritable> {

	SequenceFile.Writer writer = null;

	@Override
	protected void reduce(BowlParitionTwoKey key, Iterable<TwoNodeKey> values, Context context)
		throws IOException, InterruptedException {

	    Configuration conf = context.getConfiguration();
	    String path = conf.get("test.triangleIntermidiate2");
	    String outputPath = path + "-" + key.key1 + "-" + key.key2;
	    FileSystem fileSystem = null;

	    fileSystem = FileSystem.get(conf);

	    // 定义输出流（SequenceFile）

	    writer = SequenceFile.createWriter(fileSystem, conf, new Path(outputPath), TwoNodeKey.class,
		    IntWritable.class);

	    for (TwoNodeKey twonode : values) {
		TwoNodeKey outputResult = WritableUtils.clone(twonode, context.getConfiguration());

		writer.append(outputResult, new IntWritable(0));
	    }
	    IOUtils.closeStream(writer);
	}
    }

    public static class BowlOMapper3 extends Mapper<Object, Text, NullWritable, NullWritable> {

	;

	private Integer p;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    p = conf.getInt("test.p", 1);
	}

	private List<Integer> getProblem(Text line) {
	    StringTokenizer st = new StringTokenizer(line.toString());

	    List<Integer> _problem = new ArrayList<Integer>();
	    try {
		for (;;) {
		    _problem.add(Integer.parseInt(st.nextToken()));
		}
	    } catch (Exception localException) {
		return _problem;
	    }
	}

	private void readEdge(String path, Context context, int i, TIntObjectHashMap<TIntArrayList> edgeList)
		throws IOException {
	    Configuration conf = context.getConfiguration();

	    Path[] allPaths = org.apache.hadoop.mapreduce.filecache.DistributedCache.getLocalCacheFiles(context.getConfiguration());

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    reader = new SequenceFile.Reader(FileSystem.getLocal(conf), allPaths[i], conf);
	    OneNodeKey key = (OneNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    IntDegreeArray value = (IntDegreeArray) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    while (reader.next(key, value)) {
		edgeList.put(key.getNode(), new TIntArrayList(value.nodeArray));
	    }
	    IOUtils.closeStream(reader);
	}

	private void enumerateSubgraph(String path, Context context, int i, int j,
		TIntObjectHashMap<TIntArrayList> edgeList) throws IOException {
	    Configuration conf = context.getConfiguration();

	    SequenceFile.Reader reader = null;
	    FileSystem fs = FileSystem.get(conf);
	    reader = new SequenceFile.Reader(fs, new Path(path), conf);
	    TwoNodeKey key = (TwoNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	    IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

	    long position = reader.getPosition();
	    while (reader.next(key, value)) {
		String syncSeen = reader.syncSeen() ? "*" : "";
		position = reader.getPosition();

		TIntArrayList leftNodeList = edgeList.get(key.getNode1());
		TIntArrayList rightNodeList = edgeList.get(key.getNode2());

		if (leftNodeList != null && rightNodeList != null) {
		    // this counting is just a approximation not reliable
		    context.getCounter("test", "core_size").increment(1);
		    context.getCounter("test", "list_size").increment(leftNodeList.size() + rightNodeList.size());
		    context.getCounter("test", "pattern_size")
			    .increment((leftNodeList.size()) * (rightNodeList.size()));
		}
	    }
	    IOUtils.closeStream(reader);
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    List<Integer> problems = getProblem(value);
	    TIntObjectHashMap<TIntArrayList> edgeList = new TIntObjectHashMap<TIntArrayList>();

	    Configuration conf = context.getConfiguration();
	    String path = conf.get("test.triangleIntermidiate");

	    String inputPath = path + "-" + problems.get(0);
	    readEdge(inputPath, context, problems.get(0), edgeList);

	    inputPath = path + "-" + problems.get(1);

	    readEdge(inputPath, context, problems.get(1), edgeList);

	    String path2 = conf.get("test.triangleIntermidiate2");
	    String inputPath2 = path2 + "-" + problems.get(0) + "-" + problems.get(1);
	    enumerateSubgraph(inputPath2, context, problems.get(0), problems.get(1), edgeList);

	}
    }

	public static class BowlParitionKey implements WritableComparable<BowlParitionKey> {
		public int key1;

		public BowlParitionKey() {
			key1 = 0;

		}

		public BowlParitionKey(int key1) {
			this.key1 = key1;

		}

		public void setKey(int key1, Integer p) {
			this.key1 = Math.abs(key1) % p;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			key1 = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(key1);
		}

		@Override
		public int compareTo(BowlParitionKey other) {
			if (key1 > other.key1) {
				return 1;
			}
			if (key1 < other.key1) {
				return -1;
			}
			return 0;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + key1;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			BowlParitionKey other = (BowlParitionKey) obj;
			return key1 == other.key1;
		}

	}

	public static class BowlParitionTwoKey implements WritableComparable<BowlParitionTwoKey> {

		public int key1;
		public int key2;

		public BowlParitionTwoKey() {
			key1 = 0;
			key2 = 0;
		}

		public BowlParitionTwoKey(int key1, int key2) {
			this.key1 = key1;
			this.key2 = key2;
		}

		public void setKey(int key1, int key2, Integer p) {
			this.key1 = Math.abs(key1) % p;
			this.key2 = Math.abs(key2) % p;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			key1 = in.readInt();
			key2 = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(key1);
			out.writeInt(key2);
		}

		@Override
		public int compareTo(BowlParitionTwoKey other) {
			if (key1 > other.key1) {
				return 1;
			}
			if (key1 < other.key1) {
				return -1;
			}

			if (key2 > other.key2) {
				return 1;
			}
			if (key2 < other.key2) {
				return -1;
			}

			return 0;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + key1;
			result = prime * result + key2;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			BowlParitionTwoKey other = (BowlParitionTwoKey) obj;
			if (key1 != other.key1)
				return false;
			return key2 == other.key2;
		}
	}

    public static void main(String[] args) throws Exception {
	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();

	JobConf conf = new JobConf();
	long milliSeconds = 10000 * 60 * 60; // <default is 600000, likewise can
					     // give any value)
	conf.setLong("mapred.task.timeout", milliSeconds);
	ToolRunner.run(conf, new BowlO(), args);

	endTime = System.currentTimeMillis();
	log.info("[BowlO] Time elapsed: " + (endTime - startTime) / 1000 + "s");
    }

}
