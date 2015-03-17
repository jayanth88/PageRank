package job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import job.BlockedPR.GLOBAL_STATE;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BlockedPRReduce extends MapReduceBase 
		implements Reducer<LongWritable, Text, LongWritable, Text> {
	public static final long NUM_OF_NODES = 685230;		// TODO
	
	public void reduce(LongWritable key, Iterator<Text> values,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		
		HashMap<Long,Node> nodeInBlock = new HashMap<Long,Node>();
		HashMap<Long, Double> pageRank = new HashMap<Long, Double>();
		HashMap<Long, Double> oldPageRank = new HashMap<Long, Double>();
		HashMap<Long, Integer> degree = new HashMap<Long, Integer>();
		
		int blockSize = Block.getBlockSize((int) key.get());
		
		long fromNode = -1L;
		long toNode = -1L;
		
		double currPageRank = 0d;
		int currDegree = 0;
		
		while (values.hasNext()) {
			Text val = values.next();
			String element[] = val.toString().split("_");
			fromNode = Long.parseLong(element[0]);
			if(!element[1].equals("<NONE>")) {
				toNode = Long.parseLong(element[1]);
			}
			else {
				toNode = -1L;
			}
			currDegree = Integer.parseInt(element[2]);
			currPageRank = Double.parseDouble(element[3]);
			
			if(key.get() == Block.getBlockNumber(fromNode)) {
				if(!nodeInBlock.containsKey(fromNode)) {
					nodeInBlock.put(fromNode, new Node(fromNode));
				}
				Node currNode = nodeInBlock.get(fromNode);
				currNode.addOutEdge(toNode);
				nodeInBlock.put(fromNode, currNode);
				oldPageRank.put(fromNode, currPageRank);
			}
			
			 if((toNode!= -1) && key.get() == Block.getBlockNumber(toNode)){
				if(!nodeInBlock.containsKey(toNode)) {
					nodeInBlock.put(toNode, new Node(toNode));
				}
				Node currNode = nodeInBlock.get(toNode);
				currNode.addInEdge(fromNode);
				if(Block.getBlockNumber(fromNode) == key.get()) {
					currNode.incrementSelfEdge();
				}
				nodeInBlock.put(toNode, currNode);
			}
			pageRank.put(fromNode, currPageRank);
			degree.put(fromNode, currDegree);
		}
		
		
		boolean cont = true;
		double residue = 0.0d;
		int numOfIterations = 0;

		Heap heap = new Heap(blockSize+10);
		
		ArrayList<Long> iterationOrder = new ArrayList<Long>();
		ArrayList<NodeInEdgeWrapper> nodeArray = new ArrayList<NodeInEdgeWrapper>();
		Iterator<Long> nodeIter = nodeInBlock.keySet().iterator();
		while(nodeIter.hasNext()) {
			long nodeNum = nodeIter.next();
			Node currNode = nodeInBlock.get(nodeNum);
			nodeArray.add(new NodeInEdgeWrapper(nodeNum, currNode.getSelfInEdgeCount()));
		}
		
		while(cont) {
			numOfIterations++;
			residue = 0.0d;
			
			if(iterationOrder.size() != 0) {
				for(int i=0; i<iterationOrder.size(); i++) {
					long nodeId = iterationOrder.get(i);
					Node currNode = nodeInBlock.get(nodeId);
					
					double newPageRank = 0.0d;
					Iterator<Long> inEdgeIter = currNode.getInEdges();
					while(inEdgeIter.hasNext()) {
						long inEdgeNode =  inEdgeIter.next();
						newPageRank+=pageRank.get(inEdgeNode)/(double)degree.get(inEdgeNode);
						
					}
					newPageRank = (0.85)*newPageRank + ((0.15)/(double)NUM_OF_NODES);
					double currResidue = Math.abs(pageRank.get(nodeId) - newPageRank)/newPageRank;
					residue+=currResidue;
					pageRank.put(nodeId, newPageRank);
				}
			}
			else {
				heap.makeHeap(nodeArray);
				while(!heap.isEmpty()) {
					long nodeId = heap.removeMin().getNodeId();
					iterationOrder.add(nodeId);
					Node currNode = nodeInBlock.get(nodeId);
					
					double newPageRank = 0.0d;
					Iterator<Long> inEdgeIter = currNode.getInEdges();
					while(inEdgeIter.hasNext()) {
						long inEdgeNode =  inEdgeIter.next();
						newPageRank+=pageRank.get(inEdgeNode)/(double)degree.get(inEdgeNode);
					}
					newPageRank = (0.85)*newPageRank + ((0.15)/(double)NUM_OF_NODES);
					double currResidue = Math.abs(pageRank.get(nodeId) - newPageRank)/newPageRank;
					residue+=currResidue;
					pageRank.put(nodeId, newPageRank);
					
					Iterator<Long> outEdges = currNode.getOutEdges();
					while(outEdges.hasNext()) {
						heap.decrementEdge(outEdges.next());
					}
				}
			}
			
			
			nodeIter = nodeInBlock.keySet().iterator();
			long nodeId;
			Node currNode;
			
			while(nodeIter.hasNext()) {
				nodeId = nodeIter.next();
				currNode = nodeInBlock.get(nodeId);
				
				double newPageRank = 0.0d;
				Iterator<Long> inEdgeIter = currNode.getInEdges();
				while(inEdgeIter.hasNext()) {
					long inEdgeNode =  inEdgeIter.next();
					newPageRank+=pageRank.get(inEdgeNode)/(double)degree.get(inEdgeNode);
					
				}
				newPageRank = (0.85)*newPageRank + ((0.15)/(double)NUM_OF_NODES);
				double currResidue = Math.abs(pageRank.get(nodeId) - newPageRank)/newPageRank;
				residue+=currResidue;
				pageRank.put(nodeId, newPageRank);
			}
			
			residue = residue/(double) nodeInBlock.size();
			if(residue < 0.001) {			// TODO: This condition can be changed to a fixed iteration
				cont = false;
			}
		
			System.out.println("=== Block: "+key.get()+" Residue: "+residue+" ===");
		}
		
		nodeIter = nodeInBlock.keySet().iterator();
		long nodeId;
		Node currNode;
		
		while(nodeIter.hasNext()) {
			nodeId = nodeIter.next();
			currNode = nodeInBlock.get(nodeId);
			Iterator<Long> outEdgeIter = currNode.getOutEdges();
			while(outEdgeIter.hasNext()) {
				long outEdgeNode = outEdgeIter.next();
				String redOut = outEdgeNode+" "+currNode.getNumOfOutEdges()+" "+pageRank.get(nodeId);
				output.collect(new LongWritable(nodeId), new Text(redOut));
			}
			if(currNode.getNumOfOutEdges() == 0) {
				String redOut = "<NONE> 0 "+pageRank.get(nodeId);
				output.collect(new LongWritable(nodeId), new Text(redOut));
			}
		}
		
		double totalResidue = 0.0d;
		Iterator<Long> oldPRIter = oldPageRank.keySet().iterator();
		while(oldPRIter.hasNext()) {
			nodeId = oldPRIter.next();
			double currResidue = Math.abs(oldPageRank.get(nodeId) - pageRank.get(nodeId))/pageRank.get(nodeId);
			totalResidue+= currResidue;
		}
		long longResidue = (long)totalResidue*10000;
		reporter.incrCounter(GLOBAL_STATE.TOTAL_RESIDUE, longResidue);
		reporter.incrCounter(GLOBAL_STATE.TOTAL_ITERATIONS, numOfIterations);
	}
}
