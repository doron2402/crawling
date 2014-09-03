import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * DB Class
 */
public class DB {
 
	public Connection conn = null;
 
	public DB() {
		System.out.println("DB Object...");
	}
 
	public void connectDB() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://localhost:3306/crawler";
			conn = DriverManager.getConnection(url, "root", "");
			System.out.println("conn built");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public ResultSet runSql(String sql) throws SQLException {
		Statement sta = conn.createStatement();
		return sta.executeQuery(sql);
	}
 
	public boolean runSql2(String sql) throws SQLException {
		Statement sta = conn.createStatement();
		return sta.execute(sql);
	}
 
	public void closeConnection() {
		try {
			if (conn != null || !conn.isClosed()) {
				conn.close();
			}
		} catch (Exception e) {
			System.err.println(e);
		}
		
	}
	
	@Override
	protected void finalize() throws Throwable {
		closeConnection();
	}
}