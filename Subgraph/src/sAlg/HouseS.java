package sAlg;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sData.IntDegreeArray;
import sData.OneNodeKey;
import sData.TwoNodeKey;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

/**
 * <h1>
 * Count House
 * </h1>
 * <ul>
 * 	<li>Pattern name: House </li>
 * 	<li>Pattern: (0,1),(0,3),(0,4),(1,2),(2,3),(3,4)  </li>
 * 	<li>Core: (0,1,3)</li>
 * 	<li>Non Core Node: [2,4]</li>
 * </ul>
 * @author zhanghao
 */

public class HouseS extends Configured implements Tool {

    private static Logger log = Logger.getLogger(HouseS.class);

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

    public static class HouseMapper extends Mapper<TwoNodeKey, IntDegreeArray, NullWritable, NullWritable> {
	private Integer p = new Integer(1);
	private Integer offset = new Integer(0);
	private Integer f = new Integer(2);


	private final TIntObjectHashMap<TIntArrayList> map = new TIntObjectHashMap<TIntArrayList>();


	long Counter = 0;



	private boolean isFiltered(int node0, int node1, int node2, int node3, int node4){
			int processedNode0 = node0%4;
			int processedNode1 = node1%4;
			int processedNode2 = node2%4;
			int processedNode3 = node3%4;
			int processedNode4 = node4%4;


			int square = (((node0*31+node3)*31+node2)*31 + node1) % 10;
			int triangle = ((node0*31+node3)*31+node4) % 10;

		return square < f && triangle < f;
	}

	public void enumerate(int node0, int node1, int node2, int node3, int node4, Context context){

		if (isFiltered(node0,node1,node2,node3,node4)){
			++Counter;
		}
	}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			context.getCounter("test","enumerate").increment(Counter);
		}

		@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	    Configuration conf = context.getConfiguration();
			Integer NIBFp = conf.getInt("test.p", 1);
			Integer NIBFoffset = conf.getInt("test.offset", 0);
			f = conf.getInt("test.f",2);

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
	    // Collections.shuffle(shuffleList, new
	    // Random(System.currentTimeMillis()));

	    for (int k = 0; k < shuffleList.size(); k++) {

		reader = new SequenceFile.Reader(FileSystem.getLocal(conf), allPaths[k], conf);
		OneNodeKey key = (OneNodeKey) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		IntDegreeArray value = (IntDegreeArray) ReflectionUtils.newInstance(reader.getValueClass(), conf);

		long position = reader.getPosition();
		while (reader.next(key, value)) {
		    String syncSeen = reader.syncSeen() ? "*" : "";
		    // System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
		    // key, value);
		    position = reader.getPosition(); // beginning of next record
		    int theCopiedKey = key.getNode();

		    for (int h = 0; h < value.size; ++h) {
			int keyToAddMap = value.getNode(h);
			if (map.containsKey(keyToAddMap)) {
			    map.get(keyToAddMap).add(theCopiedKey);

			} else {
			    TIntArrayList vi = new TIntArrayList();
			    vi.add(theCopiedKey);
			    map.put(keyToAddMap, vi);
			}
		    }
		}
		IOUtils.closeStream(reader);
	    }
	}

	@Override
	protected void map(TwoNodeKey key, IntDegreeArray value,
		Mapper<TwoNodeKey, IntDegreeArray, NullWritable, NullWritable>.Context context)
		throws IOException, InterruptedException {

	    TIntHashSet theSet = new TIntHashSet();
	    TIntObjectHashMap<TIntArrayList> tp = new TIntObjectHashMap<TIntArrayList>();

	    int selected_node_id = 0;
		Boolean isEnumerating = context.getConfiguration().getBoolean("test.isEmitted", false);

	    if (key.getNode1Degree() < key.getNode2Degree()) {
		selected_node_id = 1;
	    } else if (key.getNode1Degree() == key.getNode2Degree()) {
		if (key.getNode1() < key.getNode2()) {
		    selected_node_id = 1;
		} else {
		    selected_node_id = 2;
		}
	    } else {
		selected_node_id = 2;
	    }

	    int selected_node = (selected_node_id == 1 ? key.getNode1() : key.getNode2());
	    int not_selected_node = (selected_node_id == 1 ? key.getNode2() : key.getNode1());

	    TIntArrayList theList = map.get(selected_node);
	    theSet.addAll(map.get(not_selected_node));

	    for (int j = 0; j < theList.size(); ++j) {
		int tpKey = theList.get(j);
		if (map.containsKey(tpKey)) {
		    TIntArrayList intersectList = map.get(tpKey);

		    for (int i = 0; i < intersectList.size(); ++i) {
			int node = intersectList.get(i);

			if (tp.containsKey(node)) {
			    tp.get(node).add(tpKey);
			} else {
			    if (theSet.contains(node)) {
				TIntArrayList aList = new TIntArrayList();
				aList.add(tpKey);
				tp.put(node, aList);
			    }

			}
		    }
		}
	    }

//	    TIntIterator iterator = tp.keySet().iterator();
//	    long counter = 0;
//
//	    while (iterator.hasNext()) {
//		int theKey = iterator.next();
//		if (theKey != selected_node) {
//		    TIntArrayList theValue = tp.get(theKey);
//		    for (int k = 0; k < value.size; k++) {
//
//			int valueK = value.getNode(k);
//			if (theKey != valueK) {
//			    for (int i = 0; i < theValue.size(); i++) {
//				int valueI = theValue.get(i);
//				if (valueI != valueK) {
//				    ++counter;
//				}
//			    }
//			}
//		    }
//
//		    // for the case not_selected_node = theValue.get(x)
//		    counter -= (value.size);
//		}
//	    }
//
//
//
//	    context.getCounter("test", "graph").increment(counter);


		TIntIterator iterator = tp.keySet().iterator();
		long counter = 0;

		while (iterator.hasNext()) {
			int theKey = iterator.next();

			if (theKey != selected_node) {
				TIntArrayList theValue = tp.get(theKey);
				for (int k = 0; k < value.size; k++) {

					int valueK = value.getNode(k);
					if (theKey != valueK) {
						for (int i = 0; i < theValue.size(); i++) {
							if (theValue.get(i) != not_selected_node && theValue.get(i) != value.getNode(k)) {

								//counting
								++counter;


								if (isEnumerating) {
									//enumerating
									int node0 = key.node1;
									int node1 = valueK;
									int node2 = theKey;
									int node3 = theValue.get(i);
									int node4 = key.node2;

									enumerate(node0, node1, node2, node3, node4, context);
								}


							}
						}
					}
				}
			}
		}
		context.getCounter("test", "graph").increment(counter);
	}



    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();

	for (int i = 0; i < 240; ++i) {
	    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(args[0] + "/part-r-00" + appendNumberToLengthThree(i)), getConf());
	}

	conf.set("mapreduce.map.memory.mb", "4000");
	conf.set("mapreduce.map.java.opts", "-Xmx4000M");
	conf.set("test.line", args[1]);

	Job job = new Job(conf);

	job.setJarByClass(getClass());
	job.setJobName("House");

	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(NullOutputFormat.class);

	FileInputFormat.setInputPaths(job, new Path(args[1]));

	job.setNumReduceTasks(0);

	job.setMapperClass(HouseMapper.class);

	boolean success = job.waitForCompletion(true);
	return 0;
    }

    public static void main(String[] args) throws Exception {

	long startTime = 0;
	long endTime = 0;

	startTime = System.currentTimeMillis();
	JobConf conf = new JobConf();
	// conf.setProfileEnabled(true);
	long milliSeconds = 10000 * 60 * 60; // <default is 600000, likewise can
					     // give any value)
	conf.setLong("mapred.task.timeout", milliSeconds);
	conf.setNumMapTasks(240);
	ToolRunner.run(conf, new HouseS(), args);

	endTime = System.currentTimeMillis();
	log.info("[house] Time elapsed: " + (endTime - startTime) / 1000 + "s");

    }

}
