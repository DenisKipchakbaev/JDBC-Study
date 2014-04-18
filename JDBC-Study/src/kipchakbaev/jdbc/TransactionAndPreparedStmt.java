package kipchakbaev.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TransactionAndPreparedStmt {

	public static void main(String[] args) {
				
		String url = "jdbc:mysql://localhost:3306/";
		String schema = "ocpjp";
		String user = "root";
		String password = "root";
		
		Connection connection = null;
		ResultSet rs = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		
		try {
			System.out.println("Connecting to MySQL...");
			connection = DriverManager.getConnection(url+schema, user, password);
			connection.setAutoCommit(false);
			stmt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			// Create table
			System.out.println("Create Table 'Smartphone'...");
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Smartphone (id INT NOT NULL AUTO_INCREMENT, Vendor VARCHAR(20), Phone_Model VARCHAR(20), Release_Year INT, PRIMARY KEY(id))");
			System.out.println("Table created");
			
			//Insert row
			System.out.println("Inserting Row...");
			String query = "SELECT * FROM Smartphone";
			rs = stmt.executeQuery(query);
			
			rs.moveToInsertRow();
			rs.updateString("Vendor", "Samsung");
			rs.updateString("Phone_Model", "Galaxy Note 3");
			rs.updateInt("Release_Year", 2013);
			rs.insertRow();
			
			rs.moveToInsertRow();
			rs.updateString("Vendor", "Huawei");
			rs.updateString("Phone_Model", "Ascend P6");
			rs.updateInt("Release_Year", 2013);
			rs.insertRow();
			
			rs.moveToInsertRow();
			rs.updateString("Vendor", "LG");
			rs.updateString("Phone_Model", "Nexus");
			rs.updateInt("Release_Year", 2012);
			rs.insertRow();
			
			rs.moveToInsertRow();
			rs.updateString("Vendor", "Huawei");
			rs.updateString("Phone_Model", "U8800 Neo");
			rs.updateInt("Release_Year", 2011);
			rs.insertRow();
			
			connection.commit();
			System.out.println("row inserted successfully");
			
			System.out.println("id " + "\tVendor " + "\tModel " + "\t\tYear");
			int columnCount = rs.getMetaData().getColumnCount();
			rs.beforeFirst();
			while(rs.next()){
				for(int i = 1; i <= columnCount; i++) {
					System.out.print(rs.getObject(i) + "\t");
				}
				System.out.println();
			}
			query = "SELECT * FROM Smartphone WHERE Vendor = ?";
			pstmt = connection.prepareStatement(query);
			pstmt.setString(1, "Huawei");
			rs = pstmt.executeQuery();
			System.out.println("PreparedStatement SELECT:");
			System.out.println("id " + "\tVendor " + "\tModel " + "\t\tYear");
			while(rs.next()){
				System.out.print(rs.getInt(1) + "\t");
				System.out.print(rs.getString(2) + "\t");
				System.out.print(rs.getString(3) + "\t");
				System.out.print(rs.getInt(4) + "\n");
			}
			
			System.out.println();
			System.out.println("Delete Table Smartphone...");
			stmt.executeUpdate("DROP TABLE IF EXISTS Smartphone");
			System.out.println("Table Deleted");
		} catch (SQLException e) {
			System.out.println("SQL Error");
			System.out.println("Rollback if something gone wrong...");
			try {
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
				try {
					System.out.println("Closing Resources...");
					if (connection != null) connection.close();
					if (stmt != null) stmt.close();
					if (pstmt != null) pstmt.close();
					if (rs != null) rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
	}
}
