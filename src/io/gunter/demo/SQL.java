package io.gunter.demo;

import static java.lang.System.err;
import static java.lang.System.out;

import java.lang.annotation.Retention;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.extern.slf4j.Slf4j;

/**
 * SQL<RowClass> can make database queries and return RowClass objects. Saves a
 * bit a boiler-plate JDBC code when making ad hoc queries and needing row
 * objects. Performance penalty: using reflection to load a row object adds
 * roughly 15% to execution time. An example of the parameterized type RowClass:
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
 * @param <RowClass>
 *            See example class UserRow above.
 */
@Slf4j(topic = "SQL")
public class SQL<RowClass extends Row> {

	@Retention(RetentionPolicy.RUNTIME)
	public @interface Order {
		int value();
	}

	private RowClass rowClassObj = null;
	private Connection conn = null;
	private String rowQuerySQL = null;
	private ResultSet rs = null;
	private boolean hitDb = false;
	private Field rowNumField = null;
	private int rowCount = 0;
	private String[] resultColumns = null;
	private Map<Integer, Field> columnToField = new HashMap<>();
	private String tableName = null;

	/**
	 * Create a SQL object. Use SQL.query() to return objects of type
	 * <RowClass>. - the database query contained in the 'query' field of
	 * rowClassObj. - a specified SQL query, or - a PreparedStatement.
	 */
	public SQL(RowClass rowClassObj, Connection conn) {
		this.rowClassObj = rowClassObj;
		this.conn = conn;
	}

	public static <RowClass extends Row> SQL<RowClass> query(Class<RowClass> clazz, Connection conn)
			throws InstantiationException, IllegalAccessException, SQLException {
		RowClass row = (RowClass) clazz.newInstance();
		return new SQL<RowClass>(row, conn);
	}

	@SuppressWarnings("unchecked")
	public <RowClass extends Row> SQL<RowClass> where(Object... keysAndValues) {
		String sql = getRowQuerySQL();
		StringBuilder query = new StringBuilder(sql);
		query.append(" WHERE ");
		boolean isKey = true;
		String and = "";
		for (Object obj : keysAndValues) {
			if (isKey) {
				if (obj == null) {
					throw new IllegalArgumentException("null column name");
				}
				String s = obj.toString();
				query.append(and);
				query.append(s);
			} else {
				if (obj == null) {
					query.append(" IS NULL ");
				}
				String s = obj.toString();
				if (obj instanceof String) {
					s = "'" + s + "'";
				}
				query.append(s);
			}
			isKey = !isKey;
			and = " AND ";
		}
		return (SQL<RowClass>) this;
	}

	public static <RowClass extends Row> void run(Class<RowClass> clazz, Connection conn) throws InstantiationException,
			IllegalAccessException, SQLException, NoSuchMethodException, SecurityException {
		RowClass row = (RowClass) clazz.newInstance();
		Method method = row.getClass().getMethod("action");
		//run(clazz, conn, method);
		// Consumer<RowClass> action = method;
		new SQL<RowClass>(row, conn).forEach((RowClass rc) -> {
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

	public static <RowClass extends Row> void run(Class<RowClass> clazz, Connection conn, Consumer<RowClass> action) 
	//public static <RowClass extends Row> void run(RowClass rowClassObj, Connection conn, Consumer<RowClass> action)
			throws InstantiationException, IllegalAccessException, SQLException {
		query(clazz, conn).forEach(action);
		/*
		@SuppressWarnings("unchecked")
		RowClass row = (RowClass) rowClassObj.getClass().newInstance();
		new SQL<RowClass>(row, conn).forEach(action);
		*/
	}

	void filterFields() throws IllegalArgumentException, IllegalAccessException {
		for (Field field : rowClassObj.getClass().getFields()) {
			log.debug("field name = " + field.getName());
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
						log.debug("resultColumn = " + resultColumn + ", colIndex = " + colIndex + ", index = " + index);
						if (index != -1 && colIndex == -1) {
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
		StringTokenizer st = new StringTokenizer(lowerCaseSql.substring(fromIndex + 6).trim());
		tableName = st.nextToken();
		if (!Character.isLetter(tableName.charAt(0))) {
			tableName = null;
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
	 * Use this query method for SQL query specified in RowClass
	 */
	public RowClass query() throws SQLException, InstantiationException, IllegalAccessException {
		return doQuery(null, null);
	}

	/*
	 * The first call performs the query (creating the PreparedStatement if none
	 * was passed). Every call returns a result row.
	 */
	private RowClass doQuery(PreparedStatement ps, String sql)
			throws SQLException, InstantiationException, IllegalAccessException, SecurityException {
		if (!hitDb) {
			if (sql == null) {
				sql = getRowQuerySQL();
				/*
				Field queryField = null;
				try {
					queryField = rowClassObj.getClass().getField("query");
					sql = (String) queryField.get(null);
				} catch (NoSuchFieldException e) {
					throw new IllegalArgumentException(
							"SQL.allResults() requires Row class have a public static final String query field.");
				}
				*/
				/*
				 * TODO remove: could not get to UserRow.getQuery. RowClass row
				 * = (RowClass) rowClassObj.getClass().newInstance(); sql =
				 * row.getQuery();
				 */
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

	public void insert() throws IllegalArgumentException, IllegalAccessException {
		insert(this.rowClassObj);
	}

	public void insert(RowClass row) throws IllegalArgumentException, IllegalAccessException {
		List<RowClass> rows = new ArrayList<RowClass>(1);
		rows.add(row);
		insert(rows);
	}

	public void insert(List<RowClass> rows) throws IllegalArgumentException, IllegalAccessException {
		if (tableName == null) {
//			getQueryColumnNames(rows.get(0).getQuery());
			getQueryColumnNames(getRowQuerySQL());
			filterFields();
		}
		StringBuilder query = new StringBuilder("insert into ");
		query.append(tableName);
		query.append(" (");
		String valComma = "";
		for (String columnName : resultColumns) {
			query.append(valComma);
			valComma = ",";
			query.append(columnName);
		}
		query.append(") values ");
		String rowComma = "";
		for (RowClass row : rows) {
			query.append(rowComma);
			rowComma = ",";
			query.append("(");
			valComma = "";
			for (Integer colIndex : columnToField.keySet()) {
				Field field = columnToField.get(colIndex);
				query.append(valComma);
				valComma = ",";
				String colValue = (String) field.get(row);
				if (field.getType() == String.class && colValue != null) {
					colValue = "'" + colValue + "'";
				}
				query.append(colValue);
			}
			query.append(")");
		}
		log.info("Insert query = " + query);
		// TODO run the insert!
	}


	private String getRowQuerySQL() {
		if (rowQuerySQL != null) {
			return rowQuerySQL;
		}

		Field queryField = null;
		try {
			queryField = rowClassObj.getClass().getField("query");
			rowQuerySQL = (String) queryField.get(null);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new IllegalArgumentException(
					"SQL.allResults() requires Row class have a public static final String query field.");
		}
		/*
		 * TODO remove: could not get to UserRow.getQuery. RowClass row =
		 * (RowClass) rowClassObj.getClass().newInstance(); sql =
		 * row.getQuery();
		 */
		if (rowQuerySQL == null) {
			throw new IllegalArgumentException(
					"SQL.allResults() requires either the query be passed, or the Row class must have a 'query' field.");
		}
		return rowQuerySQL;
	}

	public void close() throws SQLException {
		// if (ps != null) {
		// ps.close();
		// }
		if (conn != null) {
			conn.close();
		}
	}

}
