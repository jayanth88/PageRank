package job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import job.UnblockedPR.GLOBAL_STATE;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UnblockedPRReduce extends MapReduceBase 
		implements Reducer<LongWritable, Text, LongWritable, Text> {
	public static final long NUM_OF_NODES = 685230;		// TODO
	@Override
	public void reduce(LongWritable key, Iterator<Text> values,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
	
		ArrayList<Long> inEdge = new ArrayList<Long>();
		ArrayList<Long> outEdge = new ArrayList<Long>();
		HashMap<Long, Double> pageRank = new HashMap<Long, Double>();
		HashMap<Long, Integer> degree = new HashMap<Long, Integer>();
		
		long fromNode = 0L;
		long toNode = 0L;
		double oldPageRank = 0d;
		double currPageRank = 0d;
		int currDegree = 0;
		
		while (values.hasNext()) {
			Text val = values.next();
			String element[] = val.toString().split("_");
			fromNode = Long.parseLong(element[0]);
			if (!element[1].equals("<NONE>")) {
				toNode = Long.parseLong(element[1]);
			}
			else {
				toNode = -1;
			}
			currDegree = Integer.parseInt(element[2]);
			currPageRank = Double.parseDouble(element[3]);
			
			if(key.get() == fromNode) {
				if(toNode!=-1) {
					outEdge.add(toNode);
				}
				oldPageRank = currPageRank;
			}
			else {
				inEdge.add(fromNode);
				pageRank.put(fromNode, currPageRank);
				degree.put(fromNode, currDegree);
			}
		}
			
		Double newPageRank = 0.0;
		Iterator<Long> in = inEdge.iterator();
		while(in.hasNext()) {
			long inNode = in.next();
			newPageRank+= (double) pageRank.get(inNode) / (double) degree.get(inNode);
		}
		newPageRank = (0.85)*newPageRank + (0.15)/(double) NUM_OF_NODES;
		
		double residue = Math.abs(oldPageRank-newPageRank)/newPageRank;
		long longResidue = (long)(residue*10000);
		reporter.incrCounter(GLOBAL_STATE.TOTAL_RESIDUE, longResidue);
		Iterator<Long> out = outEdge.iterator();
		boolean emittedAtleastOne = false;
		while(out.hasNext()) {
			long outNode = out.next();
			String outVal = outNode+" "+outEdge.size()+" "+newPageRank;
			output.collect(new LongWritable(key.get()), new Text(outVal));
			emittedAtleastOne = true;
		}
		if(!emittedAtleastOne) {
			String outVal = "<NONE> 0 "+newPageRank;
			output.collect(new LongWritable(key.get()), new Text(outVal));
		}
	}
}