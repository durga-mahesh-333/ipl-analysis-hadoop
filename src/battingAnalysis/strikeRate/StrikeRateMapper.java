package strikeRate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StrikeRateMapper extends Mapper<LongWritable , Text, Text, Text>{
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
		//super.map(key, value, context);
		String[] columns = value.toString().split(",");
		int innings = Integer.parseInt(columns[INNINGS]);
		String striker = columns[STRIKER];
		int runsInThatDelivery = Integer.parseInt(columns[RUNS_IN_THAT_DELIEVRY]);
		int extras = Integer.parseInt(columns[EXTRAS]);
		int season = Integer.parseInt(columns[SEASON]);

		int ball = 0;
		if (season >= 2015 && innings < 3) {
			if(extras==0 || (extras!=0 && runsInThatDelivery!=0)) 
				ball=1;
			
			context.write(new Text(striker), new Text(runsInThatDelivery+"-"+ball));
		}
	}
}
