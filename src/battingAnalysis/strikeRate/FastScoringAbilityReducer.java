package strikeRate;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FastScoringAbilityReducer extends Reducer<Text, Text,Text, DoubleWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method s100tub
		double totalBalls=0;
		double totalRuns=0;
		double strikeRate=0;
		for(Text value : values) {
			String[] data=value.toString().split("-");
			
			if(data.length==2) {
				double runs = Integer.parseInt(data[0].toString());
				double ball = Integer.parseInt(data[1].toString());
				
				totalBalls+=ball;
				totalRuns+=runs;
			}
		}
		if(totalRuns>300 && totalBalls!=0 ) {
			strikeRate=(totalRuns/totalBalls)*100;
			DecimalFormat formatValue = new DecimalFormat("##.00");
			double formattedValue = Double.parseDouble(formatValue.format(strikeRate));
			DoubleWritable reqValue = new DoubleWritable(formattedValue);
			context.write(key, reqValue );
		}
	}
}
