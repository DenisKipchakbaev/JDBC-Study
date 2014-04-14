package kipchakbaev.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCStudy {

	public static void main(String[] args) {
		System.out.println("Connecting to MySQL...");
		
		String url = "jdbc:mysql://localhost:3306/";
		String schema = "ocpjp";
		String user = "root";
		String password = "root";
		ResultSet resultSet = null;
		
		try (Connection connection = DriverManager.getConnection(url+schema, user, password);
			 Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);) {
			
			System.out.println("DB connection successful");
			// Create table User
			System.out.println("Creating table User... (IF NOT EXISTS)");
			String createUserTable = "CREATE TABLE IF NOT EXISTS User (id INT NOT NULL AUTO_INCREMENT, fName varchar(20), lName VARCHAR(40) NOT NULL, primary key(id))";
			int result = statement.executeUpdate(createUserTable);
			System.out.println("rows updated: " + result);
			System.out.println("Table User created successfully");
			
			// Inserting some data in table User
			resultSet = statement.executeQuery("SELECT * FROM User");
			System.out.println("Inserting rows to table User");
			resultSet.moveToInsertRow();
			resultSet.updateString("fName", "John");
			resultSet.updateString("lName", "D'oh");
			resultSet.insertRow();
			
			resultSet.moveToInsertRow();
			resultSet.updateString("fName", "David");
			resultSet.updateString("lName", "OhYeah");
			resultSet.insertRow();
			
			resultSet.moveToInsertRow();
			resultSet.updateString("fName", "Catrin");
			resultSet.updateString("lName", "Chapachapa");
			resultSet.insertRow();
			
			System.out.println("Insert rows successfull");
			
			System.out.println("Reading table User...");
			System.out.println("id \tfName \tlName");
			resultSet.beforeFirst();
			while (resultSet.next()) {
				System.out.println(resultSet.getInt("id") + "\t"
						+ resultSet.getString("fName") + "\t"
						+ resultSet.getString("lName"));
			}
			
			System.out.println("Read table successfull");
			//resultSet.close();
			
			// Get ResultSet metadata
			System.out.println("Fetch table metadata...");
			ResultSetMetaData metadata = resultSet.getMetaData();
			System.out.println("Column count: " + metadata.getColumnCount());
			System.out.println("Column display size: " + metadata.getColumnDisplaySize(1));
			System.out.println("Column catalog name: " + metadata.getCatalogName(1));
			System.out.println("Column class name: " + metadata.getColumnClassName(1));
			System.out.println("Column name: " + metadata.getColumnName(1));
			System.out.println("Column type: " + metadata.getColumnType(1));
			System.out.println("Column type: " + metadata.getColumnTypeName(1));
			System.out.println("Type precision: " + metadata.getPrecision(1));
			System.out.println("Type scale (number of digits after decimal point): " + metadata.getScale(1));
			System.out.println("Schema name for column: " + metadata.getSchemaName(2));
			System.out.println("Column's table name: " + metadata.getTableName(1));
			System.out.println("Is column nullable: " + metadata.isNullable(1));
			System.out.println("Is column nullable: " + metadata.isNullable(2));
			System.out.println("Is auto increment: " + metadata.isAutoIncrement(1));
			System.out.println("Is case sensitive (INT): " + metadata.isCaseSensitive(1));
			System.out.println("Is case sensitive (VARCHAR): " + metadata.isCaseSensitive(2));
			System.out.println("Is currency: " + metadata.isCurrency(1));
			System.out.println("isDefinitelyWritable 2: " + metadata.isDefinitelyWritable(2));
			System.out.println("isReadOnly 1: " + metadata.isReadOnly(1));
			System.out.println("isSearchable 1: " + metadata.isSearchable(1));
			System.out.println("isSigned 1: " + metadata.isSigned(1));
			System.out.println("isWritable 1: " + metadata.isWritable(1));
			
			// Update row
			System.out.println("Updating row...");
			resultSet = statement.executeQuery("SELECT * FROM User Where lName='Chapachapa'");
			resultSet.absolute(1);
			resultSet.updateString("fName", "CHUPACABRA");
			resultSet.updateString("lName", "GoodDoggy");
			resultSet.updateRow();
			
			resultSet = statement.executeQuery("SELECT * FROM User");
			while (resultSet.next()) {
				System.out.println(resultSet.getInt("id") + "\t"
						+ resultSet.getString("fName") + "\t"
						+ resultSet.getString("lName"));
			}
			System.out.println("Update row successfull");
			
			// Delete row from table...
			System.out.println("Delete row from table");
			resultSet = statement.executeQuery("SELECT * FROM User WHERE fName='David'");
			if(resultSet.next()){
				resultSet.deleteRow();
			}
			//resultSet.close();
			
			resultSet = statement.executeQuery("SELECT * FROM User");
			while (resultSet.next()) {
				System.out.println(resultSet.getInt("id") + "\t"
						+ resultSet.getString("fName") + "\t"
						+ resultSet.getString("lName"));
			}
			System.out.println("Delete row successfull");
			// Delete table User
			System.out.println("Delete table User...");
			String deleteUserTable = "DROP TABLE IF EXISTS User";
			result = statement.executeUpdate(deleteUserTable);
			System.out.println("Delete table User successfull");
			
			System.out.println("Closing ResultSet");
			//resultSet.close();
			
			// Get Database metadata
			DatabaseMetaData dbMetadata = connection.getMetaData();
			System.out.println("Displaying database metadata:");
			System.out.println("CatalogSeparator: " + dbMetadata.getCatalogSeparator());
			System.out.println("Preferred term for 'catalog': " + dbMetadata.getCatalogTerm());
			System.out.println("DatabaseMajorVersion: " + dbMetadata.getDatabaseMajorVersion());
			System.out.println("DatabaseMinorVersion: " + dbMetadata.getDatabaseMinorVersion());
			System.out.println("DatabaseProductName: " + dbMetadata.getDatabaseProductName());
			System.out.println("DatabaseProductVersion: " + dbMetadata.getDatabaseProductVersion());
			System.out.println("DefaultTransactionIsolation: " + dbMetadata.getDefaultTransactionIsolation());
			System.out.println("JDBCDriverMajorVersion: " + dbMetadata.getDriverMajorVersion());
			System.out.println("JDBCDriverMinorVersion: " + dbMetadata.getDriverMinorVersion());
			System.out.println("DriverName: " + dbMetadata.getDriverName());
			System.out.println("DriverVersion: " + dbMetadata.getDriverVersion());
			System.out.println("ExtraNameCharacters: " + dbMetadata.getExtraNameCharacters());
			System.out.println("JDBCVersion: " + dbMetadata.getJDBCMajorVersion() + " " + dbMetadata.getJDBCMinorVersion());
			System.out.println("MaxColumnsInTable: " + dbMetadata.getMaxColumnsInTable());
			System.out.println("MaxConnections: " + dbMetadata.getMaxConnections());
			System.out.println("MaxRowSize: " + dbMetadata.getMaxRowSize() + " bytes");
			System.out.println("SQLKeywords: " + dbMetadata.getSQLKeywords());
			System.out.println("URL: " + dbMetadata.getURL());
			System.out.println("UserName: " + dbMetadata.getUserName());
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("SQL Exception");
			e.printStackTrace();
		} finally {
			try {
				resultSet.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Error: ResultSet close failed");
				e.printStackTrace();
			}
		}
	}

}
