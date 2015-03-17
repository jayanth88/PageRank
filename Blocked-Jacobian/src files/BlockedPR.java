package job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BlockedPR extends Configured implements Tool {

	public static enum GLOBAL_STATE {
		TOTAL_RESIDUE, TOTAL_ITERATIONS;
	}
	
	public static final long NUM_OF_NODES = 685230;		// TODO
	@Override
	public int run(String[] args) throws Exception {
		
		boolean cont=true;
		int pass=0;
		JobConf conf = null;
		
		try {
			while(cont) {
				conf = new JobConf(getConf(),getClass());
				
				if(pass==0){
					FileInputFormat.addInputPath(conf, new Path(args[0]));
//					FileInputFormat.addInputPath(conf, new Path("/home/harsh/hadoop-1.0.4/page_rank/input"));
				}
				else{
					FileInputFormat.addInputPath(conf, new Path(args[1]+"/pass"+pass));
//					FileInputFormat.addInputPath(conf, new Path("/home/harsh/AWS workspace/Copy of BlockedPR/output/pass"+pass));
				}
				FileOutputFormat.setOutputPath(conf, new Path(args[1]+"/pass"+(pass+1)));
//				FileOutputFormat.setOutputPath(conf, new Path("/home/harsh/AWS workspace/Copy of BlockedPR/output/pass"+(pass+1)));
				
				conf.setInputFormat(TextInputFormat.class);
				conf.setMapperClass(BlockedPRMap.class);
				conf.setMapOutputKeyClass(LongWritable.class);
				conf.setMapOutputValueClass(Text.class);
				
				conf.setReducerClass(BlockedPRReduce.class);
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);
				conf.setOutputFormat(TextOutputFormat.class);
				
				RunningJob job; 
				job = JobClient.runJob(conf);
					
				job.waitForCompletion();
				
				Counters counters = job.getCounters();
				Counter residue = counters.findCounter(GLOBAL_STATE.TOTAL_RESIDUE);
				Counter iterations = counters.findCounter(GLOBAL_STATE.TOTAL_ITERATIONS);
				Double avgResidue = (double) residue.getValue()/(double) NUM_OF_NODES;
				double avgIterations = (double) iterations.getValue()/(double) Block.getNumOfBlocks();
				avgResidue = avgResidue/(double) 10000;
				
				
				System.err.println("Pass: "+pass+" Average Residue Value: "+avgResidue);
				System.err.println("Pass: "+pass+" Average Iterations: "+avgIterations);
				if(avgResidue <= 0.001) {
					cont = false;
				}
				residue.setValue(0L);
				iterations.setValue(0L);
				pass++;
			}		
		}
		catch(Exception e) {
			
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BlockedPR(), args);
		System.exit(exitCode);
	}
}
