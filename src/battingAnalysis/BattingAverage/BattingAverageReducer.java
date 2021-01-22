package BattingAverage;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BattingAverageReducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		double totalRuns = 0;
		double totalNoOfOuts = 0;
		for (Text value : values) {
			String[] data = value.toString().split("-");
			if (data.length == 2) {
				double runsScored = Double.parseDouble(data[0].toString());
				double outs = Double.parseDouble(data[1].toString());
				totalRuns += runsScored;
				totalNoOfOuts += outs;
			}
		}
		
		
		//considering Batsmen who scored min 200 runs
		if (totalRuns >= 300 && totalNoOfOuts > 0) {
			double avg = totalRuns / totalNoOfOuts;
			// double value truncating upto two decimals
			DecimalFormat formatValue = new DecimalFormat("##.00");
			double formattedValue = Double.parseDouble(formatValue.format(avg));
			DoubleWritable reqValue = new DoubleWritable(formattedValue);
			context.write(key, reqValue);
		}
	}

}
