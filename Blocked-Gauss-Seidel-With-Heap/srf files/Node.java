package job;

import java.util.ArrayList;
import java.util.Iterator;

public class Node {
	private long nodeID = -1;
	private ArrayList<Long> inEdges;
	private ArrayList<Long> outEdges;
	private int selfInEdgeCount;
	
	public Node(long nodeId) {
		this.nodeID = nodeId;
		selfInEdgeCount = 0;
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
	public void incrementSelfEdge() {
		selfInEdgeCount+=1;
	}
	public int getSelfInEdgeCount() {
		return selfInEdgeCount;
	}
}

class NodeInEdgeWrapper {
	private long nodeId;
	private int edgeCount;
	
	public NodeInEdgeWrapper(long nodeId, int edgeCount) {
		this.nodeId = nodeId;
		this.edgeCount = edgeCount;
	}
	
	public NodeInEdgeWrapper() {
		
	}
	public long getNodeId() {
		return nodeId;
	}

	public void setNodeId(long nodeId) {
		this.nodeId = nodeId;
	}

	public int getEdgeCount() {
		return edgeCount;
	}

	public void setEdgeCount(int edgeCount) {
		this.edgeCount = edgeCount;
	}
	public String toString() {
		return "("+nodeId+" "+edgeCount+")";
	}
}