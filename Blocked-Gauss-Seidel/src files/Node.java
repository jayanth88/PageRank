package job;

import java.util.ArrayList;
import java.util.Iterator;

public class Node {
	private long nodeID = -1;
	private ArrayList<Long> inEdges;
	private ArrayList<Long> outEdges;
	
	public Node(long nodeId) {
		this.nodeID = nodeId;
		inEdges = new ArrayList<Long>();
		outEdges = new ArrayList<Long>();
	}
	
	public Iterator<Long> getInEdges() {
		return inEdges.iterator();
	}
	public Iterator<Long> getOutEdges() {
		return outEdges.iterator();
	}
	public void addInEdge(long node) {
		if(!inEdges.contains(node) && (node!=-1)) {
			inEdges.add(node);
		}
	}
	
	public void addOutEdge(long node) {
		if(!outEdges.contains(node) && (node!=-1)) {
			outEdges.add(node);
		}
	}
	public long getID() {
		return nodeID;
	}
	public int getNumOfOutEdges() {
		return outEdges.size();
	}
	public int getNumOfInEdges() {
		return inEdges.size();
	}
}
