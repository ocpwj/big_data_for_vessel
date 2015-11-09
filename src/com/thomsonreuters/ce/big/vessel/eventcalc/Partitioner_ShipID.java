package com.thomsonreuters.ce.big.vessel.eventcalc;

import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner_ShipID extends Partitioner<Key_ShipIDAndRecordTime, TextArrayWritable> {

	@Override
	public int getPartition(Key_ShipIDAndRecordTime arg0,
			TextArrayWritable arg1, int numReduceTasks) {
		// TODO Auto-generated method stub
		
		int hashcode=arg0.getShipID().hashCode();
		return (hashcode & Integer.MAX_VALUE) % numReduceTasks;
	}

}
