package io.gunter.demo;

import java.lang.annotation.Retention;
import static java.lang.System.out;
import static java.lang.System.err;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.event.Level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

/**
 * SQL<RowClass> can make database queries and return RowClass objects. Saves a
 * bit a boiler-plate JDBC code when making ad hoc queries and needing row
 * objects. An example of the parameterized type RowClass:
 * 
 * <pre>
 * class UserRow extends SQL.Row { // all public fields
 * 	&#64;SuppressWarnings("unused")
 * 	public static final String query = "select username, age from user";
 * 	public Integer age; // will receive data from the age column
 * 	public String name; // will receive data from the username column ('name'
 * 						// matching 'username')
 * 
 * 	public void action() {
 * 		System.out.println("row#" + rowNum + ": name=" + name + ", age: " + age + ", email: " + mail);
 * 	};
 * }
 *
 * SQL.run(new UserRow(), getConn());
 * </pre>
 * 
 * Note:
 * <ul>
 * <li>the query field and the action method are required to use SQL.run</li>
 * <li>the run method buffers all result rows and closes the Connection.</li>
 * <li>alternate query strings and action methods can be specified.</li>
 * <li>each field name must be a full or partial case-insensitive match for a
 * result column in the query, or
 * <li>a field can have a non-matching name and be identified by an @Order
 * annotation.
 * <li>rows can be retrieved one at a time.</li>
 * </ul>
 * An example of the alternate usage pattern:
 * 
 * <pre>
 * class UserRow extends SQL.Row { // all public fields
 * 	&#64;Order(value = 2)
 * 	public Integer numLoopsAroundTheSun;
 * 	&#64;Order(value = 1)
 * 	public String greeting;
 * }
 *
 * try (Connection conn = getConn();) {
 * 	UserRow row = new UserRow();
 * 	SQL<UserRow> sql = new SQL<>(row, conn);
 * 	while (null != (row = sql.query("select concat('Hi ', username) as str, age from user"))) {
 * 		System.out.println(row.greeting + ", you are " + row.numLoopsAroundTheSun + " years old");
 * 	}
 * }
 * </pre>
 * 
 * 
 * @author jaygunter
 *
 * @param <RowClass> See example class UserRow above.
 */
@Slf4j
public class SQL<RowClass> {

	@Retention(RetentionPolicy.RUNTIME)
	public @interface Order {
		int value();
	}

	public static class Row {
		public Integer rowNum;
	}

	private RowClass rowClassObj = null;
	private Connection conn = null;
	private ResultSet rs = null;
	private boolean hitDb = false;
	private Field rowNumField = null;
	private int rowCount = 0;
	private String[] resultColumns = null;
	private Map<Integer, Field> columnToField = new HashMap<>();;

	/**
	 * Create a SQL object. Use SQL.query() to return objects of type
	 * <RowClass>. - the database query contained in the 'query' field of
	 * rowClassObj. - a specified SQL query, or - a PreparedStatement.
	 */
	public SQL(RowClass rowClassObj, Connection conn) {
		this.rowClassObj = rowClassObj;
		this.conn = conn;
	}

	public static <RowClass> void run(RowClass rowClassObj, Connection conn, Consumer<RowClass> action)
			throws InstantiationException, IllegalAccessException, SQLException {
		@SuppressWarnings("unchecked")
		RowClass row = (RowClass) rowClassObj.getClass().newInstance();
		new SQL<RowClass>(row, getConn()).forEach(action);
	}

	public static <RowClass> void run(RowClass rowClassObj, Connection conn) throws InstantiationException,
			IllegalAccessException, SQLException, NoSuchMethodException, SecurityException {
		@SuppressWarnings("unchecked")
		RowClass row = (RowClass) rowClassObj.getClass().newInstance();
		Method method = rowClassObj.getClass().getMethod("action");
		// Consumer<RowClass> action = method;
		new SQL<RowClass>(row, getConn()).forEach((RowClass rc) -> {
			try {
				method.invoke(rc);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
		});
	}

	void filterFields() throws IllegalArgumentException, IllegalAccessException {
		for (Field field : rowClassObj.getClass().getFields()) {
			if (field.getName().equals("rowNum")) {
				this.rowNumField = field;
			} else if (field.getName().equals("query")) {
				// "query" field is handled in doQuery
			} else {
				int colIndex = -1;
				Order order = field.getAnnotation(Order.class);
				if (order != null) {
					// colIndex = order.value();
					columnToField.put(order.value(), field);
				} else {
					int j = 1;
					for (String resultColumn : resultColumns) {
						int index = resultColumn.indexOf(field.getName());
						if (index != -1) {
							colIndex = j;
						} else if (colIndex != -1) {
							// throw new IllegalArgumentException("Multiple
							// fields (" + field.getName() + ", "
							// + fields[colIndex].getName() + ") match query
							// result column.");

						}
						j++;
					}
					if (colIndex == -1) {
						throw new IllegalArgumentException(
								"Cannot match field " + field.getName() + " to a query result column.");
					}
					columnToField.put(colIndex, field);
				}
			}
		}
	}

	/**
	 * Returns a List of all row objects generated.
	 */
	@SuppressWarnings("unchecked")
	public RowClass[] allResults(String sql) throws SQLException, InstantiationException, IllegalAccessException {
		List<RowClass> rowList = new LinkedList<>();
		RowClass row = (RowClass) rowClassObj.getClass().newInstance();
		while (null != (row = query(sql))) {
			rowList.add(row);
		}
		row = (RowClass) rowClassObj.getClass().newInstance();
		RowClass[] rowArray = (RowClass[]) Array.newInstance(row.getClass(), rowList.size());
		rowList.toArray(rowArray);
		return rowArray;
	}

	/**
	 * Returns a List of all row objects generated.
	 */
	@SuppressWarnings("unchecked")
	public RowClass[] allResults() throws SQLException, InstantiationException, IllegalAccessException {
		List<RowClass> rowList = new LinkedList<>();
		RowClass row = (RowClass) rowClassObj.getClass().newInstance();
		while (null != (row = query(null))) {
			rowList.add(row);
		}
		row = (RowClass) rowClassObj.getClass().newInstance();
		RowClass[] rowArray = (RowClass[]) Array.newInstance(row.getClass(), rowList.size());
		rowList.toArray(rowArray);
		return rowArray;
	}

	/**
	 * Invokes the action on each row. Closes the connection.
	 * 
	 * @param action
	 *            the consumer of a row
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 */
	public void forEach(Consumer<RowClass> action) throws InstantiationException, IllegalAccessException, SQLException {
		// Arrays.stream(allResults()).forEach(action);
		Stream.of(allResults()).forEach(action);
		close();
	}

	private void getQueryColumnNames(String sqlOrig) {
		String sql = sqlOrig;
		String lowerCaseSql = sql.toLowerCase();
		int selectIndex = lowerCaseSql.indexOf("select ");
		if (-1 == selectIndex) {
			throw new IllegalArgumentException("No SELECT found in query: " + sql);
		}
		int fromIndex = lowerCaseSql.indexOf(" from ");
		if (-1 == fromIndex) {
			throw new IllegalArgumentException("No FROM found in query: " + sql);
		}
		String csvResultColumns = lowerCaseSql.substring(selectIndex + 7, fromIndex);
		// strip parens and text inside them
		while (true) {
			/*
			 * int leftParenIndex = sql.indexOf("("); if (leftParenIndex == -1)
			 * { break; }
			 */
			int leftParenIndex = -1;
			int leftParenCount = 0;
			int rightParenIndex = -1;
			int rightParenCount = 0;
			boolean go = true;
			for (int i = 0; go && i < csvResultColumns.length(); i++) {
				char c = csvResultColumns.charAt(i);
				switch (c) {
				case '(':
					if (leftParenIndex == -1) {
						leftParenIndex = i;
					}
					leftParenCount++;
					break;
				case ')':
					rightParenIndex = i;
					rightParenCount++;
					if (leftParenCount == rightParenCount) {
						csvResultColumns = csvResultColumns.substring(0, leftParenIndex)
								+ csvResultColumns.substring(rightParenIndex + 1);
						go = false;
					}
					break;
				}
			}
			if (leftParenCount != rightParenCount) {
				throw new IllegalArgumentException(
						leftParenCount + " " + rightParenCount + " Unbalanced parens in query: " + sqlOrig);
			}
			if (leftParenIndex == -1) {
				break;
			}
		}
		// We have transformed "select concat('x', col1) as xxx, sum(col2) as
		// yyy from users group by age"
		// into "concat as xxx, count as yyy". Now it is safe to split on commas
		// to get the column names.
		resultColumns = csvResultColumns.split(",");
		for (int i = 0; i < resultColumns.length; i++) {
			int asIndex = resultColumns[i].indexOf(" as ");
			if (asIndex != -1) {
				resultColumns[i] = resultColumns[i].substring(asIndex + 4);
			}
		}
	}

	/*
	 * Use this query method when ps.setXXX is used to set where-clause values.
	 */
	public RowClass query(PreparedStatement ps, String sql)
			throws SQLException, InstantiationException, IllegalAccessException {
		return doQuery(ps, sql);
	}

	/*
	 * Use this query method for SQL without parameters
	 */
	public RowClass query(String sql) throws SQLException, InstantiationException, IllegalAccessException {
		return doQuery(null, sql);
	}

	/*
	 * The first call performs the query (creating the PreparedStatement if none
	 * was passed). Every call returns a result row.
	 */
	private RowClass doQuery(PreparedStatement ps, String sql)
			throws SQLException, InstantiationException, IllegalAccessException, SecurityException {
		if (!hitDb) {
			if (sql == null) {
				Field queryField = null;
				try {
					queryField = rowClassObj.getClass().getField("query");
					sql = (String) queryField.get(null);
				} catch (NoSuchFieldException e) {
					throw new IllegalArgumentException(
							"SQL.allResults() requires Row class have a public static final String query field.");
				}
				if (sql == null) {
					throw new IllegalArgumentException(
							"SQL.allResults() requires either the query be passed, or the Row class must have a 'query' field.");
				}
			}
			getQueryColumnNames(sql);
			filterFields();
			if (ps == null) {
				ps = conn.prepareStatement(sql);
			}
			rs = ps.executeQuery();
			hitDb = true;
			rowCount = 0;
		}
		return getRow();
	}

	public RowClass getRow() throws SQLException, InstantiationException, IllegalAccessException {
		if (!rs.next()) {
			return null;
		}

		@SuppressWarnings("unchecked")
		RowClass row = (RowClass) rowClassObj.getClass().newInstance();

		for (Integer colIndex : columnToField.keySet()) {
			Field field = columnToField.get(colIndex);
			if (field.getType() == String.class) {
				field.set(row, rs.getString(colIndex));
			} else if (field.getType() == Integer.class) {
				field.set(row, rs.getInt(colIndex));
			}
			// TODO handle DATE, etc.
		}

		rowNumField.set(row, ++rowCount);
		return row;
	}

	public void close() throws SQLException {
		// if (ps != null) {
		// ps.close();
		// }
		if (conn != null) {
			conn.close();
		}
	}

	public static Connection getConn() throws SQLException {
		return DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb", "root", "");
	}

	public void action3(SQL.Row row) {
		err.println("num=" + row.rowNum);
	}

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
		 * sql.query("select username, age from user;"))) { // Row objects make
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
		 * sql.query("select username, age from user;"))) { row.print(); } }
		 * 
		 * System.out.println("Query, buffer all rows, and process...");
		 * 
		 * try (Connection conn = getConn();) { SQL<UserRow2> sql = new
		 * SQL<>(new UserRow2(), conn);
		 * Arrays.stream(sql.allResults("select username, age from user;")).
		 * forEach(UserRow2::print); }
		 */

//		log.info("Tidy: Query, buffer all rows, and process...");
		err.println("Tidy: Query, buffer all rows, and process...");

		class UserRow extends SQL.Row { // all public fields
			// optional field 'query'
			@SuppressWarnings("unused")
			public static final String query = "select username, email, age from user";
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

		SQL.run(new UserRow(), getConn()); // uses UserRow.action()
		SQL.run(new UserRow(), getConn(), UserRow::action2);
		SQL.run(new UserRow(), getConn(),
				(UserRow r) -> err.println("LAMBDA rowNum=" + r.rowNum + ", name=" + r.name));

		try (Connection conn = getConn();) {
			UserRow row = new UserRow();
			SQL<UserRow> sql = new SQL<>(row, conn);
			while (null != (row = sql.query(
					"select email, concat('aaa ', username) as name, sum(age) as age from user group by name, age, email"))) {
				//row.action();
				ObjectMapper mapper = new ObjectMapper();
				String jsonInString = mapper.writeValueAsString(row);
				out.println("json = " + jsonInString);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// TODO add ordinary (non-reflection) JDBC version of query/retrieve for
		// timing comparision.
	}

}
