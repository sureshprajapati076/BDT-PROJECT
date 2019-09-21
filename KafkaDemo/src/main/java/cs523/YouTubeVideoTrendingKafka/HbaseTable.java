package cs523.YouTubeVideoTrendingKafka;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTable {
	private static final String TABLE_NAME = "videos";
	
	private static final String	ABOUT = "CF_About";
	private static final String DETAILS = "CF_Details";
	private static final String	COMMENT ="CF_Comments";
	private static final String	SETTINGS ="CF_Settings";
	private static final String REACTION = "CF_Reaction";
	private static final String TRENDING = "CF_Trending";
	//private static final String ROW_ID ="CF_RowId";
			
	
	private static final Configuration config = HBaseConfiguration.create();

	@SuppressWarnings("deprecation")
	public static void CreateTable() throws IOException {
		
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(DETAILS));
			table.addFamily(new HColumnDescriptor(ABOUT));
			table.addFamily(new HColumnDescriptor(REACTION));
			
			table.addFamily(new HColumnDescriptor(COMMENT));
			table.addFamily(new HColumnDescriptor(SETTINGS));
			table.addFamily(new HColumnDescriptor(TRENDING));
			
			
			
			
			
			
			
			
			System.out.print("Creating table.... ");
			if (!admin.tableExists(table.getTableName())) {
		
			admin.createTable(table);
			}
			
			/*		admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			else{*/
			System.out.println(" Done!");
		}
	}
			
		@SuppressWarnings("deprecation")
		public static void addData(List<String> list) throws IOException{	
			//System.out.println(list);
			
			
			HTable hTable = new HTable(config, "videos");
			Put p1 = new Put(Bytes.toBytes(list.get(0)));
			
			p1.add(Bytes.toBytes(DETAILS), Bytes.toBytes("title"),Bytes.toBytes(list.get(2)));
			p1.add(Bytes.toBytes(DETAILS), Bytes.toBytes("published_date"),Bytes.toBytes(list.get(5)));
			p1.add(Bytes.toBytes(DETAILS), Bytes.toBytes("thumbnail_link"),Bytes.toBytes(list.get(11)));
			
			p1.add(Bytes.toBytes(ABOUT), Bytes.toBytes("description"),Bytes.toBytes(list.get(15)));
			p1.add(Bytes.toBytes(ABOUT), Bytes.toBytes("channel_title"),Bytes.toBytes(list.get(3)));
			p1.add(Bytes.toBytes(ABOUT), Bytes.toBytes("category_id"),Bytes.toBytes(list.get(4)));
			p1.add(Bytes.toBytes(ABOUT), Bytes.toBytes("tags"),Bytes.toBytes(list.get(6)));
			
			p1.add(Bytes.toBytes(REACTION), Bytes.toBytes("views"),Bytes.toBytes(list.get(7)));
			p1.add(Bytes.toBytes(REACTION), Bytes.toBytes("likes"),Bytes.toBytes(list.get(8)));
			p1.add(Bytes.toBytes(REACTION), Bytes.toBytes("dislikes"),Bytes.toBytes(list.get(9)));
			
			p1.add(Bytes.toBytes(COMMENT), Bytes.toBytes("comment_count"),Bytes.toBytes(list.get(10)));
		
			p1.add(Bytes.toBytes(SETTINGS), Bytes.toBytes("comment_disable"),Bytes.toBytes(list.get(12)));
			p1.add(Bytes.toBytes(SETTINGS), Bytes.toBytes("rating_disable"),Bytes.toBytes(list.get(13)));
			p1.add(Bytes.toBytes(SETTINGS), Bytes.toBytes("video_removed_err"),Bytes.toBytes(list.get(14)));
			
			p1.add(Bytes.toBytes(TRENDING), Bytes.toBytes("trending_date"),Bytes.toBytes(list.get(1)));
		
			
			
			

			hTable.put(p1);
		
			System.out.println("data inserted");

			hTable.close();
			
		}
	


}

