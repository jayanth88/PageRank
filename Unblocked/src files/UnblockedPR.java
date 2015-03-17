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

public class UnblockedPR extends Configured implements Tool {

	public static enum GLOBAL_STATE {
		TOTAL_RESIDUE;
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
				}
				else{
					FileInputFormat.addInputPath(conf, new Path(args[1]+"/pass"+pass));
				}
				FileOutputFormat.setOutputPath(conf, new Path(args[1]+"/pass"+(pass+1)));
				
				conf.setInputFormat(TextInputFormat.class);
				conf.setMapperClass(UnblockedPRMap.class);
				conf.setMapOutputKeyClass(LongWritable.class);
				conf.setMapOutputValueClass(Text.class);
				
				conf.setReducerClass(UnblockedPRReduce.class);
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);
				conf.setOutputFormat(TextOutputFormat.class);
				
				RunningJob job; 
				job = JobClient.runJob(conf);
					
				job.waitForCompletion();
				
				Counters counters = job.getCounters();
				Counter residue = counters.findCounter(GLOBAL_STATE.TOTAL_RESIDUE);
				Double avgResidue = (double) residue.getValue()/(double) NUM_OF_NODES;
				avgResidue = avgResidue/(double) 10000;
				
				System.err.println("Pass: "+pass+" Average Residue Value: "+avgResidue);
				if(pass > 10) {
					cont = false;
				}
				residue.setValue(0L);
				pass++;
			}		
		}
		catch(Exception e) {
			
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UnblockedPR(), args);
		System.exit(exitCode);
	}
}