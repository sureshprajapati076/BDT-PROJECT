package cs523.youtube;
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
	private static final String CF_DETAILS = "details";
	private static final String	CF_SETTINGS ="settings";
	private static final String CF_REACTION = "reaction";
	
	private static final Configuration config = HBaseConfiguration.create();

	public static void CreateTable() throws IOException {
		
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			
			table.addFamily(new HColumnDescriptor(CF_DETAILS));
			table.addFamily(new HColumnDescriptor(CF_REACTION));
			table.addFamily(new HColumnDescriptor(CF_SETTINGS));
	
			System.out.print("Creating table.... ");
			if (admin.tableExists(table.getTableName())) {
				
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			
			System.out.println(" Done!");
		}
	}
			
		@SuppressWarnings("deprecation")
		public static void addData(List<String> list) throws IOException{	
			//System.out.println(list);
			if(list.size()==16){
			
			HTable hTable = new HTable(config, "videos");
			Put p1 = new Put(Bytes.toBytes(list.get(0)));
			
			p1.add(Bytes.toBytes(CF_DETAILS), Bytes.toBytes("title"),Bytes.toBytes(list.get(2)));
			p1.add(Bytes.toBytes(CF_DETAILS), Bytes.toBytes("published_date"),Bytes.toBytes(list.get(5)));
			p1.add(Bytes.toBytes(CF_DETAILS), Bytes.toBytes("thumbnail_link"),Bytes.toBytes(list.get(11)));
			p1.add(Bytes.toBytes(CF_DETAILS), Bytes.toBytes("description"),Bytes.toBytes(list.get(15)));
			p1.add(Bytes.toBytes(CF_DETAILS), Bytes.toBytes("channel_title"),Bytes.toBytes(list.get(3)));
			p1.add(Bytes.toBytes(CF_DETAILS), Bytes.toBytes("category_id"),Bytes.toBytes(list.get(4)));
			p1.add(Bytes.toBytes(CF_DETAILS), Bytes.toBytes("tags"),Bytes.toBytes(list.get(6)));
			p1.add(Bytes.toBytes(CF_DETAILS), Bytes.toBytes("trending_date"),Bytes.toBytes(list.get(1)));
			
			p1.add(Bytes.toBytes(CF_REACTION), Bytes.toBytes("views"),Bytes.toBytes(list.get(7)));
			p1.add(Bytes.toBytes(CF_REACTION), Bytes.toBytes("likes"),Bytes.toBytes(list.get(8)));
			p1.add(Bytes.toBytes(CF_REACTION), Bytes.toBytes("dislikes"),Bytes.toBytes(list.get(9)));
			p1.add(Bytes.toBytes(CF_REACTION), Bytes.toBytes("comment_count"),Bytes.toBytes(list.get(10)));
		
			p1.add(Bytes.toBytes(CF_SETTINGS), Bytes.toBytes("comment_disable"),Bytes.toBytes(list.get(12)));
			p1.add(Bytes.toBytes(CF_SETTINGS), Bytes.toBytes("rating_disable"),Bytes.toBytes(list.get(13)));
			p1.add(Bytes.toBytes(CF_SETTINGS), Bytes.toBytes("video_removed_err"),Bytes.toBytes(list.get(14)));
			
			hTable.put(p1);
		
			System.out.println("data inserted");

			hTable.close();
			}
			else{
				System.out.println("Skipped: (size) = "+list.size()+".....>"+list);
			}	
		}
}
