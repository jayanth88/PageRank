package job;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UnblockedPRMap extends MapReduceBase 
		implements Mapper<LongWritable, Text, LongWritable, Text>{
	public static final long NUM_OF_NODES = 685230;		// TODO
	public void map(LongWritable key, Text value,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
	
		String valueInStr = value.toString();
		String line [] = valueInStr.split("\t");
		long fromNode = -1L;
		long toNode = -1L;
		
		String temp [] = line[1].split(" ");
		fromNode = Long.parseLong(line[0]);
		if(!temp[0].equals("<NONE>")) {
			toNode = Long.parseLong(temp[0]);
		}
		else {
			toNode = -1;
		}
		StringBuffer redText = new StringBuffer();
		redText.append(fromNode).append("_");
		if(toNode!=-1) {
			redText.append(toNode).append("_");
		}
		else {
			redText.append("<NONE>").append("_");
		}
		redText.append(temp[1]).append("_");
		redText.append(temp[2]);
		output.collect(new LongWritable(fromNode), new Text(redText.toString()));
		if (toNode!=-1) {
			output.collect(new LongWritable(toNode), new Text(redText.toString()));
		}
	}
}



