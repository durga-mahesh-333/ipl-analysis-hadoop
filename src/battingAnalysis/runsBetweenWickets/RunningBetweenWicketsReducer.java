package runsBetweenWickets;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RunningBetweenWicketsReducer extends Reducer<Text, Text,Text, DoubleWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method s100tub
		double totalBallsPlayed=0;
		double totalRunsByFours=0;
		double totalRunsBySixes=0;
		double totalRuns=0;
		double runsBetweenWickets=0;
		double totalBoundaryBalls=0;
		double totalRunsByBoundaries;
		
		for(Text value : values) {
			String[] data=value.toString().split("-");
			
			if(data.length==4) {
				double four = Integer.parseInt(data[0].toString());
				double six = Integer.parseInt(data[1].toString());
				double ball = Integer.parseInt(data[2].toString());
				double runs = Integer.parseInt(data[3].toString());
				
				totalBallsPlayed+=ball;
				totalRunsByFours+=four*4;
				totalRunsBySixes+=six*6;
				totalBoundaryBalls+=four+six;
				totalRuns+=runs;
			}
		}
		
		totalRunsByBoundaries=totalRunsByFours+totalRunsBySixes;
		runsBetweenWickets=totalRuns - totalRunsByBoundaries;
		double ballsWithOutBoundaries =totalBallsPlayed - totalBoundaryBalls;
		if(totalRuns>300 && ballsWithOutBoundaries!=0 ) {
			double runsBetweenWicketsPerBall=(runsBetweenWickets/ballsWithOutBoundaries);
			DecimalFormat formatValue = new DecimalFormat("##.000");
			double formattedValue = Double.parseDouble(formatValue.format(runsBetweenWicketsPerBall));
			DoubleWritable reqValue = new DoubleWritable(formattedValue);
			context.write(key, reqValue );
		}
	}
}
