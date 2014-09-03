import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

 
public class Main {
	public static DB db = new DB();
	public static String URL_TO_SCAN = "http://xfinitytv.comcast.net/movie.widget";
	public static String URL_VIEW_DATE = "http://stats.grok.se/json/en/";
	public static final int NUMBER_OF_THREADS = 30;
	public static void main(String[] args) throws SQLException, IOException, JSONException {
		db.connectDB();
		db.runSql2("TRUNCATE records;");
		processXfinitytvComcastPage(URL_TO_SCAN);
//		System.out.println(getMoviesByTotalViews(0,4));
	}
	
	/*
	 * processXfinitytvComcastPage
	 * crawl movies from xfinitytv.comcast.net (URL_TO_SCAN)
	 */
	public static void processXfinitytvComcastPage (String URL) throws SQLException, IOException, JSONException{
		Document doc = Jsoup.connect(URL).get();
		Elements movies = doc.select("a[href]");
		ExecutorService threadPool = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
		for(Element link: movies){
			
			String xfinityID = null;
			if (link.attr("id") != null) {
				xfinityID =link.attr("id");
				 
			}
			String sql = "select * from records where xfinityID = "+xfinityID+"";
			ResultSet rs = db.runSql(sql);
			if(xfinityID != null && rs.next()) {
				System.out.println("Duplicate");
			} else {
				sql = "INSERT INTO  `crawler`.`records` " + "(`RecordID`,`xfinityID`,`url`,`name`, `type`) VALUES " + "(?,?,?,?,?);";
				PreparedStatement stmt = db.conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
				stmt.setString(1, null);
				stmt.setString(2, xfinityID.toString());//xfinity id
				stmt.setString(3, link.attr("href")); // URL
				stmt.setString(4, link.attr("data-t")); // movie name
				stmt.setString(5, link.attr("data-type")); //Type (Movie)
				stmt.execute();
				ResultSet rs2 = stmt.getGeneratedKeys();
				rs2.next();
				final int colId = rs2.getInt(1);
				if (link.attr("data-t") != null) {
					final String movie_name = link.attr("data-t");
					threadPool.execute(new Runnable() {
						@Override
						public void run() {
							try {
								Main.getNumberOfViewPerMonth(colId, movie_name, "201408");
							} catch (Exception ex) {
								ex.printStackTrace();
							}
						}
					});
				}
			}
		}
		try {
			threadPool.awaitTermination(0, null);
		} catch (InterruptedException ex) {
			
		}
		db.closeConnection();
	}
	
	/*
	 * getNumberOfViewPerMonth
	 * scan for daily views
	 */
	public static void getNumberOfViewPerMonth(int movieID, String movieName, String movieDate) throws SQLException, IOException, JSONException {
		String url = URL_VIEW_DATE + movieDate + "/" + URLEncoder.encode(movieName, "utf-8");
		System.out.println("Fetching: " + url);
		try {
			URLConnection connection = new URL(url).openConnection();
			InputStream is = connection.getInputStream();
			JSONTokener tokener = new JSONTokener(is);
			JSONObject root = new JSONObject(tokener);
			JSONObject dailyViews = root.getJSONObject("daily_views");
			Iterator<?> i = dailyViews.keys();
			int sum = 0;
			while (i.hasNext()) {
				String key = (String) i.next();
				int tmpValue = dailyViews.getInt(key);
				if (tmpValue > 0) {
					sum += tmpValue;
				}
			}
			
			String sql = "UPDATE `crawler`.`records` SET total_view = "+sum+" WHERE RecordID = "+movieID+";";
			PreparedStatement stmt = db.conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			stmt.execute();
		} catch (FileNotFoundException e) {
			// TODO: handle exception
		}
	}

	/*
	 * return a json object for api or whatever we want to return {movies: [{id: 123, xfinity_id: 123, name: 'bla', total_views: 123123},..]}
	 * limitStart >= 0 , limitEnd > limitStart || 0
	 */
	public static JSONObject getMoviesByTotalViews(int limitStart, int limitEnd) throws SQLException, IOException, JSONException {
		if (limitEnd <= 0 || limitStart < 0 || limitStart >= limitEnd) {
			limitEnd = 10;
			limitStart = 0;
		}
		String sql = "SELECT `RecordID`, `xfinityID`, `name`, `total_view` FROM `crawler`.`records` ORDER BY `total_view` DESC LIMIT "+limitStart+","+limitEnd+";";
		ResultSet rs = db.runSql(sql);
		JSONObject root = new JSONObject();
		JSONArray movies = new JSONArray();
		while (rs.next()) {
			JSONObject movie = new JSONObject();
			System.out.println(rs.getString("name"));
			movie.put("id", rs.getInt(1));
			movie.put("xfinity_id", rs.getLong("xfinityID"));
			movie.put("name", rs.getString("name"));
			movie.put("total_views", rs.getInt("total_view"));
			movies.put(movie);
		}
		root.put("movies", movies);
		return root;
	}


}