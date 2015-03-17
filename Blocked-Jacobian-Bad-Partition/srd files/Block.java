package job;

public class Block {

	public static long getBlockNumber(long node) {
        return (node%getNumOfBlocks());
}
	public static int getNumOfBlocks() {
		return 68;
	}
	
//	public static void main(String args []) throws Exception {
//		System.out.println("Len: "+blockMap.length);
//		for(int i=0;i<blockMap.length;i++) {
//			long block = getBlockNumber(blockMap[i]-1);
//			System.out.println("Block: "+block+" Node: "+(blockMap[i]-1));
//			block = getBlockNumber(blockMap[i]);
//			System.out.println("Block: "+block+" Node: "+blockMap[i]);
//			block = getBlockNumber(blockMap[i]+1);
//			System.out.println("Block: "+block+" Node: "+(blockMap[i]+1));
//		}
//	}
}
