package BattingAverage;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BattingAverageMapper extends Mapper<LongWritable, Text, Text, Text> {
	public final int BALL = 0;
	public final int INNINGS = 1;
	public final int DELIVERY = 2;
	public final int BATTING_TEAM = 3;
	public final int STRIKER = 4;
	public final int NON_STRIKER = 5;
	public final int BOWLER = 6;
	public final int RUNS_IN_THAT_DELIEVRY = 7;
	public final int EXTRAS = 8;
	public final int DISMISSAL_TYPE = 9;
	public final int DISMISSED_PLAYER = 10;
	public final int TEAM_1 = 11;
	public final int TEAM_2 = 12;
	public final int DATE = 13;
	public final int SEASON = 14;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// super.map(key, value, context);
		String[] columns = value.toString().split(",");
		int innings = Integer.parseInt(columns[INNINGS]);
		
		String striker = columns[STRIKER];
		int runsInThatDelivery = Integer.parseInt(columns[RUNS_IN_THAT_DELIEVRY]);
		String dismissalType = columns[DISMISSAL_TYPE];
		String dismissedPlayer = columns[DISMISSED_PLAYER];
		int season = Integer.parseInt(columns[SEASON]);

		int outs = 0;
		int runsScored = 0;
		// cosidering stats since 2015
		// innings 3 and 4 comes under SUper over
		if (season >= 2015 && innings < 3) {
			String batsman = striker;
			runsScored = runsInThatDelivery;

			if (!Objects.isNull(batsman) && !batsman.isEmpty() && batsman.equals(dismissedPlayer)
					&& !dismissalType.equalsIgnoreCase("retired hurt") && !dismissalType.equalsIgnoreCase("run out")) {
				outs = 1;
			}
			context.write(new Text(batsman), new Text(runsScored + "-" + outs));

			// below code is for the batsmen who got runout 
			if (dismissalType.equalsIgnoreCase("run out")) {
				context.write(new Text(dismissedPlayer), new Text(0 + "-" + 1));
			}
		}
	}
}
