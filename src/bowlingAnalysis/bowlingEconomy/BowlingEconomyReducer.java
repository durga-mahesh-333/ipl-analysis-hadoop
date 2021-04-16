package bowlingEconomy;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BowlingEconomyReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		double totalRunsConceded=0;
		double totalBalls=0;
		double bowlingEconomy=0;
		for(Text value : values) {
			String[] data=value.toString().split("-");
			if(data.length==2) {
				double runsGiven = Integer.parseInt(data[0].toString());
				double ballsBowled = Integer.parseInt(data[1].toString());

				totalRunsConceded+=runsGiven;
				totalBalls+=ballsBowled;
			}
		}
		
		if(totalBalls>300) {
			bowlingEconomy = (totalRunsConceded/totalBalls)*6;
			DecimalFormat formatValue = new DecimalFormat("##.00");
			double formattedValue = Double.parseDouble(formatValue.format(bowlingEconomy));
			DoubleWritable reqValue = new DoubleWritable(formattedValue);
			context.write(key, reqValue);	
		}
	}
}
