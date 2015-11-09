package com.thomsonreuters.ce.big.vessel.eventcalc;


import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator_ShipID extends WritableComparator {
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Key_ShipIDAndRecordTime k1 = (Key_ShipIDAndRecordTime)a;
		Key_ShipIDAndRecordTime k2 = (Key_ShipIDAndRecordTime)b;
		
		
		VLongWritable ShipID1=k1.getShipID();	
		VLongWritable ShipID2=k2.getShipID();
				
		int cmp = ShipID1.compareTo(ShipID2);
		
		return cmp;		
		
	}

	public GroupComparator_ShipID()
	{
		super(Key_ShipIDAndRecordTime.class,true);
	}
	
	
}
