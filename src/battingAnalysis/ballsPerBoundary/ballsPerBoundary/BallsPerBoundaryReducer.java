package ballsPerBoundary;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BallsPerBoundaryReducer extends Reducer<Text, Text,Text, DoubleWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method s100tub
		double totalBalls=0;
		double totalBoundaries=0;
		double totalRuns=0;
		double ballsPerBoundary=0;
		for(Text value : values) {
			String[] data=value.toString().split("-");
			
			if(data.length==3) {
				double boundary = Integer.parseInt(data[0].toString());
				double ball = Integer.parseInt(data[1].toString());
				double runs = Integer.parseInt(data[2].toString());
				
				totalBalls+=ball;
				totalBoundaries+=boundary;
				totalRuns+=runs;
			}
		}
		if(totalRuns>300 && totalBoundaries!=0 ) {
			ballsPerBoundary=(totalBalls/totalBoundaries);
			DecimalFormat formatValue = new DecimalFormat("##.00");
			double formattedValue = Double.parseDouble(formatValue.format(ballsPerBoundary));
			DoubleWritable reqValue = new DoubleWritable(formattedValue);
			context.write(key, reqValue );
		}
	}
}
