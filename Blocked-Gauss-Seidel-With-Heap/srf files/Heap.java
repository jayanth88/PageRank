package job;

import java.util.ArrayList;

public class Heap {
	NodeInEdgeWrapper heap[];
	int heapSize;
	
	Heap(int size) {
		heapSize = 0;
		heap = new NodeInEdgeWrapper[size];
		heap[0] = new NodeInEdgeWrapper();
		heap[0].setEdgeCount((-1)*Integer.MAX_VALUE);
	}
	
	void insert(NodeInEdgeWrapper element) {
		heapSize++;
		heap[heapSize] = element;
		int now = heapSize;
		while(heap[now/2].getEdgeCount() > element.getEdgeCount()) {
			heap[now] = heap[now/2];
			now /= 2;
		}
		heap[now] = element;
	}
	
	void makeHeap(ArrayList<NodeInEdgeWrapper> nodeArray) {
		for(int i=0;i<nodeArray.size();i++) {
			this.insert(nodeArray.get(i));
		}
	}

	NodeInEdgeWrapper removeMin() {
		NodeInEdgeWrapper minElement = heap[1];
		NodeInEdgeWrapper lastElement = heap[heapSize--];
		int now = 1;
		int child = -1;
		for(now = 1; (now*2) <= heapSize; now = child) {
			child = now*2;
			if(child!=heapSize && heap[child+1].getEdgeCount() < heap[child].getEdgeCount()) {
				child++;
			}
			if(lastElement.getEdgeCount() > heap[child].getEdgeCount()) {
				heap[now] = heap[child];
			}
			else {
				break;
			}
		}
		heap[now] = lastElement;
		return minElement;
	}
	
	boolean decrementEdge(long node) {
		int location = -1;
		for(int i=1; i<heapSize+1;i++) {
			if(heap[i].getNodeId() == node) {
				location = i;
				break;
			}
		}
		if(location!=-1) {
			if(heap[location].getEdgeCount() > 0) {
				heap[location].setEdgeCount(heap[location].getEdgeCount()-1);
			}
			else {
				return false;
			}
			
			int now = location;
			NodeInEdgeWrapper toMove = heap[location];
			
			while(heap[now/2].getEdgeCount() > toMove.getEdgeCount()) {
				heap[now] = heap[now/2];
				now /= 2;
			}
			heap[now] = toMove;
//			print();
			return true;
		}
		
		return false;
	}
	public boolean isEmpty() {
		return heapSize == 0;
	}
	public void print() {
		for (int i=0;i<heapSize+1;i++) {
			System.out.println(i+" --> "+heap[i]);
		}
	}
}

//class Main {
//	public static void main(String args[]) {
//		int array [] = {3,3,0,1,2,2,5,6};
//		ArrayList<NodeInEdgeWrapper> nodeArray = new ArrayList<NodeInEdgeWrapper>();
//		for(int i=0;i<array.length;i++) {
//			nodeArray.add(new NodeInEdgeWrapper((long)i, array[i]));
//		}
//		Heap myheap = new Heap(10);
//		myheap.makeHeap(nodeArray);
//		NodeInEdgeWrapper temp;
//		temp = myheap.removeMin();
//		System.out.println("Node: "+temp.getNodeId()+" ECount: "+temp.getEdgeCount());
//		System.out.println("Dec 2: "+myheap.decrementEdge((long)2));
//		temp = myheap.removeMin();
//		System.out.println("Node: "+temp.getNodeId()+" ECount: "+temp.getEdgeCount());
//		System.out.println("Dec 0: "+myheap.decrementEdge((long)0));
//		System.out.println("Dec 0: "+myheap.decrementEdge((long)0));
////		myheap.decrementEdge(0);
////		myheap.decrementEdge(0);
//		temp = myheap.removeMin();
//		System.out.println("Node: "+temp.getNodeId()+" ECount: "+temp.getEdgeCount());
////		myheap.print();
//		System.out.println("Getting all out...");
//		while(!myheap.isEmpty()) {
//			temp = myheap.removeMin();
//			System.out.println("Node: "+temp.getNodeId()+" ECount: "+temp.getEdgeCount());
//		}
//	}
//}