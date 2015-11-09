package com.thomsonreuters.ce.big.vessel.eventcalc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_EventCalc extends
		Reducer<Key_ShipIDAndRecordTime, TextArrayWritable, VLongWritable, Text> {

	@Override
	protected void reduce(Key_ShipIDAndRecordTime key,Iterable<TextArrayWritable> rowcontent, Context ctx)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		VLongWritable shipID=key.getShipID();
		
		
		for (TextArrayWritable thislocation : rowcontent)
		{ 
			
			Text[] content=(Text[])thislocation.get();
			String Latitude=content[16].toString();
			String Longitude=content[15].toString();
			String Speed=content[18].toString();
			String Destination=content[9].toString();
			String Timestamp=content[21].toString();
			
			
			
			
		}
		
	}

}
