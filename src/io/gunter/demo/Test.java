package io.gunter.demo;

import static java.lang.System.err;
import static java.lang.System.out;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.extern.slf4j.Slf4j;

@Slf4j(topic = "SQL")
public class Test {

	public Test() {
		// TODO Auto-generated constructor stub
	}

	public static Connection getConn() throws SQLException {
		return DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb", "root", "");
	}

	/*
	 * public void action3(Row row) { err.println("num=" + row.rowNum); }
	 */

	public static void main(String args[]) throws SQLException, InstantiationException, IllegalAccessException,
			JsonProcessingException, ClassNotFoundException, NoSuchMethodException, SecurityException {

		Class.forName("com.mysql.jdbc.Driver");

		/*
		 * System.out.
		 * println("Use explicit ordering to map obj fields to query result columns.\nProcess one row at a time..."
		 * );
		 * 
		 * class UserRow extends SQL.Row { // all public fields
		 * 
		 * @Order(value = 2) public Integer num;
		 * 
		 * @Order(value = 1) public String str; }
		 * 
		 * try (Connection conn = getConn();) { UserRow row = new UserRow();
		 * SQL<UserRow> sql = new SQL<>(row, conn); while (null != (row =
		 * sql.query("select user_name, age from user;"))) { // Row objects make
		 * it easy to pass data to other methods, // and to generate JSON.
		 * System.out.println("row#" + row.rowNum + ": str=" + row.str +
		 * ", num: " + row.num); try { // TODO // ObjectMapper mapper = new
		 * ObjectMapper(); // String jsonInString =
		 * mapper.writeValueAsString(row); } catch (Exception e) {
		 * e.printStackTrace(); } } }
		 * 
		 * System.out.
		 * println("Use naming convention to map obj fields to query result columns.\nProcess one row at a time..."
		 * );
		 * 
		 * class UserRow2 extends SQL.Row { // all public fields public Integer
		 * age; public String name;
		 * 
		 * public void print() { System.out.println("row#" + rowNum + ": name="
		 * + name + ", age: " + age); }; }
		 * 
		 * try (Connection conn = getConn();) { UserRow2 row = new UserRow2();
		 * SQL<UserRow2> sql = new SQL<>(row, conn); while (null != (row =
		 * sql.query("select user_name, age from user;"))) { row.print(); } }
		 * 
		 * System.out.println("Query, buffer all rows, and process...");
		 * 
		 * try (Connection conn = getConn();) { SQL<UserRow2> sql = new
		 * SQL<>(new UserRow2(), conn);
		 * Arrays.stream(sql.allResults("select user_name, age from user;")).
		 * forEach(UserRow2::print); }
		 */

		// log.info("Tidy: Query, buffer all rows, and process...");
		err.println("Tidy: Query, buffer all rows, and process...");

		/*
		class UserRow extends Row { // all public fields
			//@Override
			public static String getQuery() {
				return "select user_name, email, age from user";
			}

			public Integer age;
			public String name;
			public String mail;

			// optional 'action' will be used by SQL.run(RowObj,Connection)
			public void action() {
				out.println("row#" + rowNum + ": name=" + name + ", age: " + age + ", email: " + mail);
			};

			// alternate action for use with
			// SQL.run(RowObj,Connection,Consumer<RowClass>)
			public void action2() {
				out.println("ACTION2: row#" + rowNum + ": name=" + name + ", age: " + age + ", email: " + mail);
			};
		}
		*/

		if (false) {
			SQL.run(UserRow.class, getConn()); // uses UserRow.action()
			SQL.run(UserRow.class, getConn(), UserRow::action2);
			SQL.run(UserRow.class, getConn(), (UserRow r) -> err.println("LAMBDA rowNum=" + r.rowNum + ", userName=" + r.userName));
		}
		
		/*
		UserRow.where("name", "new1").stream(getConn());
		UserRow.query(getConn()).where("name", "new1").stream();
		*/
		//SQL.query(UserRow.class, getConn()).where("name", "new1").stream();
		//SQL.query(UserRow.class, getConn()).where("name=?", "new1").stream();

		int maxRuns = 0;

		if (maxRuns > 0) {
			runReflecting(maxRuns);
			runStandard(maxRuns);
		}
			runReflecting(2);
			
		

		try (Connection conn = getConn();) {
			UserRow row = new UserRow();
			row.userName = "newA";
			row.password = "newA";
			SQL<UserRow> sql = new SQL<>(row, conn);
			sql.insert();
			//SQL.insert(row, conn);
		}

		UserRow row = new UserRow();
		row.userName = "newB";
		row.password = "newB";
		row.save(getConn());

		/*
		log.info(SQL.camelToUnderscore("myID"));
		log.info(SQL.camelToUnderscore("myId"));
		log.info(SQL.camelToUnderscore("myAJAXCall"));
		log.info(SQL.camelToUnderscore("AbcDEFghi"));
		log.info(SQL.camelToUnderscore("AbcDefGHIJk"));
		log.info(SQL.camelToUnderscore("AbcDEfGHIjk"));
		log.info(SQL.camelToUnderscore("AbcDefGHIjk"));
		log.info(SQL.underscoreToCamel("user_id", true));
		log.info(SQL.underscoreToCamel("user_id", false));
		*/
		log.info(SQL.camelToUnderscore("myCatwingIsWarmRow"));
		log.info(SQL.underscoreToCamel("my_catwing_is_warm", true));
		log.info("END RUN");
	}

	public static void runReflecting(int maxRuns) {
		int millisecs = 0;
		int numRows = 0;
		for (int runs = 1; runs <= maxRuns; runs++) {
			Date start = new Date();
			try (Connection conn = getConn();) {
				UserRow row = new UserRow();
				SQL<UserRow> sql = new SQL<>(row, conn);
				while (null != (row = sql.query(
						// "select email, concat('aaa ', username) as name,
						// sum(age) as age from user group by name, age,
						// email"))) {
						"select email, password, user_name, age as age from user"))) {
					numRows++;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			Date end = new Date();
			long elapsed = (end.getTime() - start.getTime());
			log.info("el=" + elapsed);
			millisecs += elapsed;
		}
		log.info("numRows = " + numRows);
		log.info("Ref avg millis = " + (1000 * millisecs / maxRuns));
	}

	public static void runStandard(int maxRuns) {
		int millisecs = 0;
		for (int runs = 1; runs <= maxRuns; runs++) {
			Date start = new Date();
			try (Connection conn = getConn();) {
				PreparedStatement preparedStatement = conn.prepareStatement(
						// "select email, concat('aaa ', username) as name,
						// sum(age) as age from user group by name, age,
						// email");
						"select user_name, password, email, age as age from user");
				ResultSet rs = preparedStatement.executeQuery();
				while (rs.next()) {
					// String userid = rs.getString("name");
					// String username = rs.getString("age");
					String username = rs.getString(1);
					Integer age = rs.getInt(2);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			Date end = new Date();
			long elapsed = (end.getTime() - start.getTime());
			// log.info("el=" + elapsed);
			millisecs += elapsed;
		}
		log.info("Std avg millis = " + (1000 * millisecs / maxRuns));
	}

}
