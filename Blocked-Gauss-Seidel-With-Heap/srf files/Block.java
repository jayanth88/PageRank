package job;

public class Block {
	public static final long blockMap[] = {0,10328, 20373, 30629, 40645, 50462, 60841, 70591,
		80118, 90497,100501, 110567, 120945, 130999, 140574, 150953, 161332, 171154,
		181514, 191625, 202004, 212383, 222762, 232593, 242878, 252938, 263149, 273210,
		283473, 293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929, 374236,
		384554, 394929, 404712, 414617, 424747, 434707, 444489, 454285, 464398, 474196,
		484050, 493968, 503752, 514131, 524510, 534709, 545088, 555467, 565846, 576225,
		586604, 596585, 606367, 616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230};

	public static long getBlockNumber(long node) {
		int left = 0;
		int right = blockMap.length-1;
		int mid = -1;
		while((right-left)>1) {
			mid = (left+right)/2;

			if(node > blockMap[mid]) {
				left = mid;
			}
			else if(node < blockMap[mid]) {
				right  = mid;
			}
			else if(node == blockMap[mid]) {
				return mid;
			}
		}
		return left;
	}
	public static int getNumOfBlocks() {
		return 68;
	}
	public static int getBlockSize(int blockNum) {
		int size = (int)blockMap[blockNum+1]-(int)blockMap[blockNum]-1;
		return size;
	}
	public static long getBlockBase(int blockNum) {
		return blockMap[blockNum];
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
