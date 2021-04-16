package bowlingStrikeRate;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BowlingStrikeRateMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public final int INNINGS = 1;
	public final int BOWLER = 6;
	public final int EXTRAS = 8;
	public final int DISMISSAL_TYPE = 9;
	public final int DISMISSED_PLAYER = 10;
	public final int SEASON = 14;
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] columns = value.toString().split(",");
		int innings = Integer.parseInt(columns[INNINGS]);		
		String bowler = columns[BOWLER];
		int extras = Integer.parseInt(columns[EXTRAS]);
		String dismissalType = columns[DISMISSAL_TYPE];
		int season = Integer.parseInt(columns[SEASON]);
		
		int noOfDismissal = 0;
		int ballBowled=0;
		// cosidering stats since 2015
		// innings 3 and 4 comes under Super over
		if (season >= 2015 && innings < 3) {
			if (Arrays.asList("bowled" , "caught" , "caught and bowled" , "stumped" , "lbw" ,"hit wicket" ).contains(dismissalType)) {
				noOfDismissal = 1;
			}
			if(extras==0) {
				ballBowled=1;
			}
			context.write(new Text(bowler), new Text( noOfDismissal+"-"+ballBowled));

		}
		
	}

}
