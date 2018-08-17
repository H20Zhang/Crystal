package sDeprecated;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.TwoNodeKey;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestVcore extends Configured implements Tool {

	private String createSeedFile(String seedFile, int p) throws IOException {
		Configuration conf = getConf();

		List<String> seedList = new ArrayList();

		FSDataOutputStream seedOut = FileSystem.get(conf).create(new Path(seedFile));

		for (int i = 0; i < 20; i++) {
			String appendString = "";
			for (int j = 0; j < 12; j++) {
				appendString += (appendNumberToLengthThree(i * 12 + j) + " ");
			}
			appendString += "\n";
			seedList.add(appendString);
		}

		Collections.shuffle(seedList);

		System.out.printf("[Info] %d subproblem(s) to process\n", Integer.valueOf(seedList.size()));

		for (int i = 0; i < 1000; ++i) {
			for (String s : seedList) {
				seedOut.writeBytes(s);
			}
		}

		seedOut.close();

		return seedFile;
	}

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

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		// create seed file

		String seed = args[0] + ".seed";

		createSeedFile(seed, 20);
		// testfile();

		conf.set("mapreduce.map.memory.mb", "10000");
		conf.set("mapreduce.map.java.opts", "-Xmx8000M");
		conf.set("mapreduce.map.cpu.vcores", "10");
		conf.set("mapreduce.reduce.cpu.vcores", "10");

		Job job = new Job(conf);

		job.setJarByClass(getClass());
		job.setJobName("Preprocess");

		job.getConfiguration().setInt("mapred.line.input.format.linespermap", 1);

		FileInputFormat.setInputPaths(job, new Path(seed));

		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		job.setNumReduceTasks(0);

		// MultithreadedMapper.setMapperClass(job, TestVcoreMapper.class);
		// MultithreadedMapper.setNumberOfThreads(job, 8);

		job.setMapperClass(TestVcoreMapper.class);

		boolean success = job.waitForCompletion(true);
		return 0;
	}

	public void testfile() throws IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(getConf());
		Path path = new Path("/user/tri1/subgraph/line_fd/part-r-00000");
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition(); // beginning of next record
			}
		} finally {
			IOUtils.closeStream(reader);
		}

	}

    public static class TestVcoreMapper extends Mapper<IntWritable, Text, NullWritable, NullWritable> {
	private final TwoNodeKey aKey = new TwoNodeKey();
	private final TwoNodeKey aValue = new TwoNodeKey();
	private final ConcurrentHashMap<OneNodeKey, Vector<OneNodeKey>> map = new ConcurrentHashMap<OneNodeKey, Vector<OneNodeKey>>();

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

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    final Configuration conf = context.getConfiguration();

	    // final int p = new Integer(conf.get("test.p"));
	    // final int offset = new Integer(conf.get("test.offset"));
	    final String path = "/user/tri1/subgraph/line_fd/part-r-00";
	    ExecutorService fixedThreadPool = Executors.newFixedThreadPool(20);

	    List<Thread> threadList = new ArrayList<Thread>();
	    for (int i = 0; i < 20; ++i) {
		Thread aThread = new Thread() {
		    @Override
		    public void run() {
			for (int i = 0; i < 10000000; ++i) {
			    for (int j = 0; j < 1012032103; ++j) {
				System.out.print(i + j);
			    }
			}
			}
		};
		threadList.add(aThread);
		aThread.start();
	    }

	    for (int i = 0; i < threadList.size(); ++i) {
		Thread aThread = threadList.get(i);
		aThread.join();
	    }

	    for (int i = 0; i < 20; i++) {
		final int j = i;

		fixedThreadPool.submit(new Runnable() {
		    @Override
		    public void run() {
			SequenceFile.Reader reader = null;

			for (int i = 0; i < 10000000; ++i) {
			    for (int j = 0; j < 1012032103; ++j) {
				System.out.print(i + j);
			    }
			}

			try {
			    FileSystem fs = FileSystem.get(conf);
			    for (int k = 0; k < 12; k++) {
				reader = new SequenceFile.Reader(fs,
					new Path("/user/tri1/subgraph/line_fd/part-r-00000"), conf);
				OneNodeKey key = (OneNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				IntDegreeArray value = (IntDegreeArray) ReflectionUtils
					.newInstance(reader.getValueClass(), conf);

				long position = reader.getPosition();
				while (reader.next(key, value)) {
				    String syncSeen = reader.syncSeen() ? "*" : "";
				    // System.out.printf("[%s%s]\t%s\t%s\n",
				    // position, syncSeen, key, value);
				    position = reader.getPosition(); // beginning
								     // of next
								     // record

				    for (int h = 0; h < value.size; ++h) {
					OneNodeKey keyToAddMap = new OneNodeKey();
					keyToAddMap.set(value.getNode(h), value.getDegree(h));
					if (map.containsKey(keyToAddMap)) {
					    map.get(value.getNode(h)).addElement(key);
					} else {
					    Vector<OneNodeKey> vi = new Vector<OneNodeKey>();
					    vi.addElement(key);
					    map.put(keyToAddMap, vi);
					}
				    }
				}
			    }
			} catch (Exception e) {
			    e.printStackTrace();
			} finally {
			    IOUtils.closeStream(reader);
			}
			}
		});
	    }
	    fixedThreadPool.shutdown();
	    fixedThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {

	    setup(context);
	    try {

		while (context.nextKeyValue()) {
		    ExecutorService fixedThreadPool = Executors.newFixedThreadPool(12);
		    Writable key = null;
		    Text value = null;

		    key = context.getCurrentKey();
		    value = context.getCurrentValue();

		    List<Integer> problem = getProblem(value);

		    final String path = "/user/tri1/tri_fd/part-r-00";
		    final Configuration conf = context.getConfiguration();
		    for (int i = 0; i < problem.size(); i++) {

			if (problem.get(i) == 239) {
			    continue;
			}

			final String fi = appendNumberToLengthThree(problem.get(i));
			fixedThreadPool.execute(new Runnable() {
			    @Override
			    public void run() {
				SequenceFile.Reader reader = null;
				try {
				    FileSystem fs = FileSystem.get(conf);
				    reader = new SequenceFile.Reader(fs, new Path(path + fi), conf);
				    TwoNodeKey key = (TwoNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(),
					    conf);
				    IntDegreeArray value = (IntDegreeArray) ReflectionUtils
					    .newInstance(reader.getValueClass(), conf);

				    OneNodeKey tpKey = new OneNodeKey();
				    long position = reader.getPosition();
				    while (reader.next(key, value)) {
					HashMap<OneNodeKey, ArrayList<OneNodeKey>> tp = new HashMap<OneNodeKey, ArrayList<OneNodeKey>>();

					for (int j = 0; j < value.size; ++j) {
					    tpKey.set(value.getNode(j), value.getDegree(j));
					    if (map.containsKey(tpKey)) {
						Vector<OneNodeKey> intersectList = map.get(tpKey);

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

				    }
				} catch (Exception e) {
				    e.printStackTrace();
				} finally {
				    IOUtils.closeStream(reader);
				}
			    }
			});
		    }
		    fixedThreadPool.shutdown();
		    fixedThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
		}
	    } finally {
		cleanup(context);
	    }
	}

    }

    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf();
	ToolRunner.run(conf, new TestVcore(), args);
    }

}
