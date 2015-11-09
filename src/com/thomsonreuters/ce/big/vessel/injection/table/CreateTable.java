package com.thomsonreuters.ce.big.vessel.injection.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Pair;

public class CreateTable {
	
	  private static Configuration conf = null;
	  private static Connection connection = null;	
	
	  private static void printTableRegions(String Spacename, String tableName) throws IOException { // co CreateTableWithRegionsExample-1-PrintTable Helper method to print the regions of a table.
		    System.out.println("Printing regions of table: " + tableName);
		    TableName tn = TableName.valueOf(Spacename,tableName);
		    RegionLocator locator = connection.getRegionLocator(tn);
		    Pair<byte[][], byte[][]> pair = locator.getStartEndKeys(); // co CreateTableWithRegionsExample-2-GetKeys Retrieve the start and end keys from the newly created table.
		    for (int n = 0; n < pair.getFirst().length; n++) {
		      byte[] sk = pair.getFirst()[n];
		      byte[] ek = pair.getSecond()[n];
		      System.out.println("[" + (n + 1) + "]" +
		        " start key: " +
		        (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) + // co CreateTableWithRegionsExample-3-Print Print the key, but guarding against the empty start (and end) key.
		        ", end key: " +
		        (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
		    }
		    locator.close();
		  }	

	public static void main(String[] args) throws IOException, InterruptedException {
	    conf = HBaseConfiguration.create();

	    connection = ConnectionFactory.createConnection(conf);
	    Admin admin = connection.getAdmin();

	    ///////////////////////////////////////////////
	    // create namespace for cdb vessel solution
	    //NamespaceDescriptor namespace = NamespaceDescriptor.create("cdb_vessel").build();
	    //admin.createNamespace(namespace);

	    TableName tableName = TableName.valueOf("cdb_vessel", "vessel_location");
	    HTableDescriptor desc = new HTableDescriptor(tableName);

	    HColumnDescriptor coldef = new HColumnDescriptor(
	      Bytes.toBytes("details"));
	    desc.addFamily(coldef);

	    admin.createTable(desc,Bytes.toBytes(0000000000L),Bytes.toBytes(9999999999L),4);
	    // ^^ CreateTableWithNamespaceExample

	    boolean avail = admin.isTableAvailable(tableName);
	    System.out.println("Table available: " + avail);
	    printTableRegions("cdb_vessel","vessel_location");
	  }

}
