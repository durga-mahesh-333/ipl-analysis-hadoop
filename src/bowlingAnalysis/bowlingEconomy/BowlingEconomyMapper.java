package bowlingEconomy;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BowlingEconomyMapper extends Mapper<LongWritable, Text, Text, Text>{
	public final int INNINGS = 1;
	public final int BOWLER = 6;
	public final int RUNS_IN_THAT_DELIEVRY = 7;
	public final int EXTRAS = 8;
	public final int SEASON = 14;
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] columns = value.toString().split(",");
		int innings = Integer.parseInt(columns[INNINGS]);		
		String bowler = columns[BOWLER];
		int runsInThatDelivery = Integer.parseInt(columns[RUNS_IN_THAT_DELIEVRY]);
		int extras = Integer.parseInt(columns[EXTRAS]);
		int season = Integer.parseInt(columns[SEASON]);
		
		int runsConceded= 0;
		int ballBowled=0;
		// cosidering stats since 2015
		// innings 3 and 4 comes under SUper over
		if (season >= 2015 && innings < 3) {
			runsConceded+=runsInThatDelivery+extras;
			if(extras==0) {
				ballBowled=1;
			}
			context.write(new Text(bowler), new Text(runsConceded + "-"+ballBowled));
		}
		
	}
}
