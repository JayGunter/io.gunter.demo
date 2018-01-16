package io.gunter.demo;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.Consumer;

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
public class SQL<RowClass extends Row<?>> implements AutoCloseable {

	@Retention(RetentionPolicy.RUNTIME)
	public @interface Order {
		int value();
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface Select {
		String value();
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface PK {
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface Version {
		boolean dbGenerated() default false;
	}

	/*
	 * TODO investigate integrating with JCS
	 * 
	 * @Retention(RetentionPolicy.RUNTIME) public @interface Cache { int
	 * maxRows() default 0; }
	 * 
	 * public static class RowCache<RowClass> { int maxRows = 0; HashMap<Object,
	 * RowClass> cache = new HashMap<>(); } private HashMap<Class,
	 * HashMap<Object, RowClass>> rowCache = new HashMap<>();
	 */

	private static class RowClassInfo {
		String[] resultColumns = null;
		Map<Integer, Field> columnToField = new HashMap<>();
		private Field rowNumField = null;
		Field primaryKeyField = null;
		String primaryKeyCamelName = null;
		String primaryKeyUnderscoreName = null;
		Field versionKeyField = null;
		String versionKeyCamelName = null;
		String versionKeyUnderscoreName = null;
		// TODO remove? would hold old value if XxxRow.id was set to another
		// value
		// private Object primaryKeyValue = null;
		String tableName = null;
		String querySQL = null;
	}
	private String rowQuerySQL = null;

	private RowClassInfo rowClassInfo = null;
	private static HashMap<Class<? extends Row<?>>, RowClassInfo> rowClassInfoCache = new HashMap<>();

	private Class<? extends Row<?>> rowClass = null;
	private RowClass rowClassObj = null;
	private Connection conn = null;
	private PreparedStatement ps = null;
	private ResultSet rs = null;
	private boolean hitDb = false;
	private int rowCount = 0;
	/*
	 * private String[] resultColumns = null; private Map<Integer, Field>
	 * columnToField = new HashMap<>(); private Field primaryKeyField = null;
	 * private String primaryKeyCamelName = null; private String
	 * primaryKeyUnderscoreName = null; private Field versionKeyField = null;
	 * private String versionKeyCamelName = null; private String
	 * versionKeyUnderscoreName = null; // TODO remove? would hold old value if
	 * XxxRow.id was set to another value // private Object primaryKeyValue =
	 * null; private String tableName = null;
	 */

	/**
	 * Create a SQL object. Use SQL.query() to return objects of type
	 * <RowClass>. - the database query contained in the 'query' field of
	 * rowClassObj. - a specified SQL query, or - a PreparedStatement.
	 * 
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	// public SQL(Row<RowClass> rowClassObj, Connection conn) {
	@SuppressWarnings("unchecked")
	public SQL(RowClass rowClassObj, Connection conn) throws IllegalArgumentException, IllegalAccessException {
		this.rowClassObj = (RowClass) rowClassObj;
		this.rowClass = (Class<? extends Row<?>>) rowClassObj.getClass();
		this.conn = conn;
		log.info("ctor rowClassObj");
		rowClassInfo = rowClassInfoCache.get(rowClass);
		if (rowClassInfo == null) {
			this.getQueryColumnNames(getRowQuerySQL());
			this.filterFields();
		}
	}

	public SQL(Class<RowClass> clazz, Connection conn) throws IllegalArgumentException, IllegalAccessException {
		this.rowClass = clazz;
		this.conn = conn;
		log.info("ctor rowClass");
		rowClassInfo = rowClassInfoCache.get(rowClass);
		if (rowClassInfo == null) {
			this.getQueryColumnNames(getRowQuerySQL());
			this.filterFields();
		}
	}

	public static <RowClass extends Row<?>> SQL<RowClass> create(Class<RowClass> clazz, Connection conn)
			throws IllegalArgumentException, IllegalAccessException {
		return new SQL<RowClass>(clazz, conn);
	}

	public static <RowClass extends Row<?>> SQL<RowClass> query(Class<RowClass> clazz, Connection conn)
			throws InstantiationException, IllegalAccessException, SQLException {
		return create(clazz, conn);
		// RowClass row = (RowClass) clazz.newInstance();
		// return new SQL<RowClass>(row, conn);
	}

	/**
	 * Supplies values for the WHERE clause used by this SQL object.
	 * 
	 * For example, <code>
	 * &#64;Select(query="select first_name from Person where first_name = ?")
	 * class PersonRow extends Row {
	 * 		public String firstName;
	 * 		public void action() {
	 * 			System.out.println("row#" + rowNum + ": fname=" + fname + ", count: " + count);
	 * 		}
	 * }
	 * SQL.query(MyRow.class, conn).whereValues("Jay").run();
	 * </code>
	 * 
	 * NOTE: SQL syntax in WHERE clauses require a test for a NULL column value
	 * be expressed as WHERE COLUMN_X IS NULL. The where method handles this
	 * case, but the whereValues method will not.
	 * 
	 * NOTE: If your SQL query does not contain a WHERE clause, do not use this
	 * method. Use the where method instead.
	 * 
	 * @param values
	 *            A varargs list of objects to be used as values in the WHERE
	 *            clause.
	 * @return this SQL object (to allow chaining to the non-static run method)
	 * @throws SQLException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	public SQL<RowClass> whereValues(Object... values)
			throws SQLException, IllegalArgumentException, IllegalAccessException {
		String sql = getRowQuerySQL();
		/*
		 * ps = conn.prepareStatement(sql); int colCount = 1; for (Object obj :
		 * values) { ps.setObject(colCount++, obj); }
		 */
		ps = buildStmt(sql, values);

		return (SQL<RowClass>) this;
	}

	private PreparedStatement buildStmt(String sql, Object... whereValues) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(sql);
		int colCount = 1;
		for (Object obj : whereValues) {
			ps.setObject(colCount++, obj);
		}
		return ps;
	}

	/**
	 * Appends a WHERE clause to the SQL query (either passed to the SQL.query()
	 * method or specified via an @Select annotation on the XxxRow class that
	 * extends the base Row class.
	 * 
	 * For example, <code>
	 * &#64;Select(query="select first_name from Person")
	 * class PersonRow extends Row {
	 * 		public String firstName;
	 * 		public void action() {
	 * 			System.out.println("row#" + rowNum + ": fname=" + fname + ", count: " + count);
	 * 		}
	 * }
	 * SQL.query(MyRow.class, conn).where("firstName", "Jay").run();
	 * </code>
	 * 
	 * NOTE: If your SQL query already contains a WHERE clause, do not use this
	 * method. Use the whereValues method instead.
	 * 
	 * @param keysAndValues
	 *            A varargs list of alternating key, value, key, value, ....
	 *            with each key being a String and the following value being a
	 *            value Object.
	 * @return this SQL object (to allow chaining to the non-static run method)
	 */
	public SQL<RowClass> where(Object... keysAndValues) {
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
				} else {
					query.append(" = ");
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
		rowQuerySQL = query.toString();

		return (SQL<RowClass>) this;
	}

	public String getPkCamelName() {
		return rowClassInfo.primaryKeyCamelName;
	}

	public String getPkUnderscoreName() {
		return rowClassInfo.primaryKeyUnderscoreName;
	}

	public Object getPkValue() {
		try {
			return rowClassInfo.primaryKeyField.get(rowClassObj);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			log.error("Cannot access value of primaryKeyField");
			return null;
		}
	}

	public void run() throws InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException,
			SQLException {
		Method method = rowClass.getMethod("action");
		try (SQL<RowClass> sql = this) {
			sql.forEach((RowClass rc) -> {
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
	}

	/**
	 * 
	 * @param clazz
	 *            e.g. UserRow (where UserRow extends Row)
	 * @param conn
	 *            a DB connection to be closed upon completion.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	public static <RowClass extends Row<?>> void run(Class<RowClass> clazz, Connection conn)
			throws InstantiationException, IllegalAccessException, SQLException, NoSuchMethodException,
			SecurityException {
		RowClass row = (RowClass) clazz.newInstance();
		Method method = row.getClass().getMethod("action");
		// run(clazz, conn, method);
		// Consumer<RowClass> action = method;
		// new SQL<Row<RowClass>>(row, conn).forEach((RowClass rc) -> {
		try (SQL<RowClass> sql = new SQL<RowClass>(row, conn)) {
			sql.forEach((RowClass rc) -> {
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
	}

	public static <RowClass extends Row<?>> void run(Class<RowClass> clazz, Connection conn, Consumer<RowClass> action)
			throws InstantiationException, IllegalAccessException, SQLException {
		query(clazz, conn).forEach(action);
	}

	public static <RowClass extends Row<?>> RowClass getById(Class<RowClass> clazz, Object id, Connection conn)
			throws InstantiationException, IllegalAccessException, SQLException {
		try (SQL<RowClass> sql = new SQL<>(clazz, conn)) {
			//sql.getQueryColumnNames(sql.getRowQuerySQL());
			//sql.filterFields();
			sql.where(camelToUnderscore(sql.rowClassInfo.primaryKeyField.getName()), id);
			List<RowClass> rows = sql.allResults();
			switch (rows.size()) {
			case 0:
				return null;
			case 1:
				return rows.get(0);
			default:
				throw new IllegalArgumentException("Found " + rows.size() + " rows for id " + id);
			}
		}
	}

	/**
	 * TODO document field-column name mapping and use of Order annotation. TODO
	 * rename this method to ... mapFieldsToColumns
	 * 
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	void filterFields() throws IllegalArgumentException, IllegalAccessException {
		// for (Field field : rowClassObj.getClass().getFields()) {
		for (Field field : rowClass.getFields()) {
			log.debug("field name = " + field.getName());
			if (field.getName().equals("rowNum")) {
				rowClassInfo.rowNumField = field;
			} else {
				int colIndex = -1;
				Order order = field.getAnnotation(Order.class);
				if (order != null) {
					// colIndex = order.value();
					rowClassInfo.columnToField.put(order.value(), field);
				} else {
					int j = 1;
					for (String resultColumn : rowClassInfo.resultColumns) {
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
					rowClassInfo.columnToField.put(colIndex, field);
				}
				if (colIndex != -1) {
					if (field.getName().equals("id") || field.isAnnotationPresent(PK.class)) {
						rowClassInfo.primaryKeyField = field;
						rowClassInfo.primaryKeyCamelName = underscoreToCamel(field.getName(), false);
						rowClassInfo.primaryKeyUnderscoreName = camelToUnderscore(field.getName());
						// primaryKeyValue = field.get(rowClassObj);
					} else if (field.isAnnotationPresent(Version.class)) {
						rowClassInfo.versionKeyField = field;
						rowClassInfo.versionKeyCamelName = underscoreToCamel(field.getName(), false);
						rowClassInfo.versionKeyUnderscoreName = camelToUnderscore(field.getName());
					}
				}
			}
		}
	}

	/**
	 * Runs specified query. Returns a List of all row objects generated.
	 */
	@SuppressWarnings("unchecked")
	public List<RowClass> allResults(PreparedStatement stmt)
			throws SQLException, InstantiationException, IllegalAccessException {
		List<RowClass> rowList = new LinkedList<>();
		RowClass row = (RowClass) rowClass.newInstance();
		while (null != (row = queryStatement(stmt))) {
			rowList.add(row);
		}
		return rowList;
	}

	/**
	 * Runs specified query. Returns a List of all row objects generated.
	 */
	public List<RowClass> allResultsStatement() throws SQLException, InstantiationException, IllegalAccessException {
		return allResults(ps);
		/*
		 * List<RowClass> rowList = new LinkedList<>(); RowClass row =
		 * (RowClass) rowClass.newInstance(); while (null != (row =
		 * queryStatement(ps))) { rowList.add(row); } return rowList;
		 */
	}

	/**
	 * Runs specified query. Returns a List of all row objects generated.
	 */
	@SuppressWarnings("unchecked")
	public List<RowClass> allResults(String sql) throws SQLException, InstantiationException, IllegalAccessException {
		List<RowClass> rowList = new LinkedList<>();
		// RowClass row = (RowClass) rowClassObj.getClass().newInstance();
		RowClass row = (RowClass) rowClass.newInstance();
		while (null != (row = query(sql))) {
			rowList.add(row);
		}
		return rowList;
	}

	/**
	 * Runs RowClass query. Returns a List of all row objects generated.
	 */
	public List<RowClass> allResults() throws SQLException, InstantiationException, IllegalAccessException {
		if (ps != null) {
			return allResultsStatement();
		}
		return allResults(getRowQuerySQL());
	}

	/**
	 * Invokes the action on each row. Closes the connection.
	 * 
	 * @param action
	 *            the consumer of a row, a method
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 */
	public void forEach(Consumer<RowClass> action) throws InstantiationException, IllegalAccessException, SQLException {
		// Stream.of(allResults()).forEach(action);
		allResults().stream().forEach(action);
		close();
	}

	private void getQueryColumnNames(String sqlOrig) {
		log.info("sqlOrig = " + sqlOrig);
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
		rowClassInfo.tableName = st.nextToken();
		if (!Character.isLetter(rowClassInfo.tableName.charAt(0))) {
			rowClassInfo.tableName = null;
		}
		log.info("tableName = " + rowClassInfo.tableName);
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
		rowClassInfo.resultColumns = csvResultColumns.split(",");
		for (int i = 0; i < rowClassInfo.resultColumns.length; i++) {
			int asIndex = rowClassInfo.resultColumns[i].indexOf(" as ");
			if (asIndex != -1) {
				rowClassInfo.resultColumns[i] = rowClassInfo.resultColumns[i].substring(asIndex + 4);
			}
			rowClassInfo.resultColumns[i] = underscoreToCamel(rowClassInfo.resultColumns[i], false);
		}
	}

	/*
	 * TODO remove? Use this query method when ps.setXXX is used to set
	 * where-clause values.
	 */
	public RowClass queryStatement(PreparedStatement stmt)
			throws SQLException, InstantiationException, IllegalAccessException {
		if (stmt == null) {
			throw new IllegalArgumentException("PreparedStatement is null.");
		}
		return doQuery(stmt, null);
	}

	/**
	 * TODO fix inconsistency: other query methods return single row for each
	 * call. Use this query method for SQL with parameters.
	 * 
	 * @whereClause examples: "x = 1", "x = ?"
	 * @whereValues values for question marks in whereClause
	 * @return List of all RowClass objects produced by the query.
	 */
	public List<RowClass> query(String whereClause, Object... whereValues)
			throws SQLException, InstantiationException, IllegalAccessException {
		if (!"where".equals(whereClause.trim().toLowerCase().split(" ")[0])) {
			whereClause = "where " + whereClause;
		}
		String sql = getRowQuerySQL() + " " + whereClause;
		ps = buildStmt(sql, whereValues);
		List<RowClass> rowList = new LinkedList<>();
		RowClass row = null;
		while (null != (row = queryStatement(ps))) {
			rowList.add(row);
		}
		return rowList;
	}

	/*
	 * Use this query method for SQL without parameters. The first call performs
	 * the query (creating the PreparedStatement if none was passed). Every call
	 * returns a result row.
	 */
	public RowClass query(String sql) throws SQLException, InstantiationException, IllegalAccessException {
		if (sql == null) {
			throw new IllegalArgumentException("SQL query string is null.");
		}
		return doQuery(null, sql);
	}

	/*
	 * Use this query method for SQL query specified in RowClass. The first call
	 * performs the query (creating the PreparedStatement if none was passed).
	 * Every call returns a result row.
	 */
	public RowClass query() throws SQLException, InstantiationException, IllegalAccessException {
		return doQuery(null, null);
	}

	/*
	 * The first call performs the query (creating the PreparedStatement if none
	 * was passed). Every call returns a result row.
	 */
	private RowClass doQuery(PreparedStatement stmt, String sql)
			throws SQLException, InstantiationException, IllegalAccessException, SecurityException {
		if (!hitDb) {
			if (stmt != null) {
				ps = stmt;
			}
			if (ps == null) {
				if (sql == null) {
					sql = getRowQuerySQL();
					if (sql == null) {
						throw new IllegalArgumentException(
								"SQL: requires either a PreparedStatement or a String query.");
					}
				}
				//getQueryColumnNames(sql);
				//filterFields();
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
		// RowClass row = (RowClass) rowClassObj.getClass().newInstance();
		RowClass row = (RowClass) rowClass.newInstance();

		for (Integer colIndex : rowClassInfo.columnToField.keySet()) {
			Field field = rowClassInfo.columnToField.get(colIndex);
			if (field.getType() == String.class) {
				field.set(row, rs.getString(colIndex));
			} else if (field.getType() == Integer.class) {
				field.set(row, rs.getInt(colIndex));
			}
			// TODO handle DATE, etc.
		}

		rowClassInfo.rowNumField.set(row, ++rowCount);
		row.setInDb(true);
		return row;
	}

	public void insert() throws IllegalArgumentException, IllegalAccessException, SQLException {
		insert(this.rowClassObj);
	}

	public void insert(RowClass row) throws IllegalArgumentException, IllegalAccessException, SQLException {
		if (row == null) {
			throw new IllegalArgumentException("Cannot insert null row object.");
		}
		List<RowClass> rows = new ArrayList<RowClass>(1);
		rows.add(row);
		insert(rows);
	}

	public void insert(List<RowClass> rows) throws IllegalArgumentException, IllegalAccessException, SQLException {
		if (rows == null) {
			throw new IllegalArgumentException("Cannot insert null list of row objects.");
		}
		if (rows.size() == 0) {
			throw new IllegalArgumentException("Cannot insert empty list of row objects.");
		}
		if (rowClassInfo == null) {
			getQueryColumnNames(getRowQuerySQL());
			filterFields();
		}
		StringBuilder query = new StringBuilder("insert into ");
		query.append(rowClassInfo.tableName);
		query.append(" (");
		String comma = "";
		StringBuilder valuePlaceholders = new StringBuilder(") values (");
		for (String columnName : rowClassInfo.resultColumns) {
			query.append(comma);
			valuePlaceholders.append(comma);
			comma = ",";
			query.append(camelToUnderscore(columnName));
			valuePlaceholders.append("?");
		}
		query.append(valuePlaceholders);
		query.append(")");
		log.info("Insert query = " + query);
		try (PreparedStatement ps = conn.prepareStatement(query.toString(), Statement.RETURN_GENERATED_KEYS)) {
			for (RowClass row : rows) {
				for (Integer colIndex : rowClassInfo.columnToField.keySet()) {
					Field field = rowClassInfo.columnToField.get(colIndex);
					Object value = field.get(row);
					log.info("field name = " + field.getName() + ", value = " + value);
					Version version = field.getAnnotation(Version.class);
					if (version != null && !version.dbGenerated()) {
						if (field.getType() == Integer.class) {
							value = value != null ? value : 0;
							field.set(row, value);
							ps.setObject(colIndex, value);
						} else if (field.getType() == Date.class) {
							value = value != null ? value : new Date();
							field.set(row, value);
							ps.setObject(colIndex, value);
						} else {
							throw new IllegalArgumentException(
									rowClass.getName() + " version column is not of type Integer or java.util.Date");
						}
					} else {
						ps.setObject(colIndex, value);
					}
				}
				ps.addBatch();
			}
			StringBuilder failedRows = new StringBuilder("");
			int rowCount = 0;
			int failedCount = 0;
			int[] rowInsertCounts = ps.executeBatch();
			// log.info("ric.len="+rowInsertCounts.length);
			ResultSet generatedIds = ps.getGeneratedKeys();
			for (int oneOnSuccess : rowInsertCounts) {
				if (rowCount == 10) {
					failedRows.append("...");
				}
				// log.info("rowCount="+rowCount+", " + oneOnSuccess);
				if (oneOnSuccess != 1) {
					failedCount++;
					failedRows.append(" ");
					failedRows.append(rowCount);
				} else {
					// log.info("next...");
					if (generatedIds.next()) {
						int id = generatedIds.getInt(1);
						// log.info("id="+ id);
						// log.info("pkf.name="+primaryKeyField.getName());
						RowClass row = rows.get(rowCount);
						rowClassInfo.primaryKeyField.set(row, id);
						row.setInDb(true);
					}
				}
				rowCount++;
			}
			if (failedCount > 0) {
				throw new SQLException(
						"Failed to insert " + failedCount + " of " + rowCount + " rows.  Failed rows: " + failedRows);
			}
		} finally {
			close();
		}
	}

	public void update() throws IllegalArgumentException, IllegalAccessException, SQLException {
		update(this.rowClassObj);
	}

	public void update(RowClass row) throws IllegalArgumentException, IllegalAccessException, SQLException {
		if (row == null) {
			throw new IllegalArgumentException("Cannot update null row object.");
		}
		if (rowClassInfo == null) {
			getQueryColumnNames(getRowQuerySQL());
			filterFields();
		}
		StringBuilder query = new StringBuilder("update ");
		query.append(rowClassInfo.tableName);
		query.append(" set ");
		String comma = "";
		for (String columnName : rowClassInfo.resultColumns) {
			query.append(comma);
			comma = ",";
			query.append(camelToUnderscore(columnName));
			query.append(" = ?");
		}
		query.append(" where ");
		query.append(camelToUnderscore(rowClassInfo.primaryKeyField.getName()));
		query.append(" = ?");
		log.info("Update query = " + query);
		try (PreparedStatement ps = conn.prepareStatement(query.toString(), Statement.RETURN_GENERATED_KEYS)) {
			int colCount = 1;
			for (Integer colIndex : rowClassInfo.columnToField.keySet()) {
				Field field = rowClassInfo.columnToField.get(colIndex);
				log.info("ci=" + colIndex + ", field name = " + field.getName() + ", value = " + field.get(row));
				Version version = field.getAnnotation(Version.class);
				if (version != null && !version.dbGenerated()) {
					if (field.getType() == Integer.class) {
						Integer intVersion = (Integer) field.get(row);
						ps.setObject(colIndex, intVersion + 1);
					} else if (field.getType() == Date.class) {
						ps.setObject(colIndex, new Date());
					} else {
						throw new IllegalArgumentException(
								rowClass.getName() + " version column is not of type Integer or java.util.Date");
					}
				} else {
					ps.setObject(colIndex, field.get(row));
				}
				colCount++;
			}
			log.info("cc=" + colCount);
			ps.setObject(colCount, rowClassInfo.primaryKeyField.get(row));
			if (1 != ps.executeUpdate()) {
				throw new SQLException("Failed:  " + query.toString());
			}
		} finally {
			close();
		}
	}

	public void delete() throws IllegalArgumentException, IllegalAccessException, SQLException {
		delete(this.rowClassObj);
	}

	public void delete(RowClass row) throws IllegalArgumentException, IllegalAccessException, SQLException {
		if (row == null) {
			throw new IllegalArgumentException("Cannot delete null row object.");
		}
		if (rowClassInfo == null) {
			getQueryColumnNames(getRowQuerySQL());
			filterFields();
		}
		StringBuilder query = new StringBuilder("delete from ");
		query.append(rowClassInfo.tableName);
		query.append(" where ");
		query.append(rowClassInfo.primaryKeyUnderscoreName);
		query.append(" = ?");
		log.info("Delete query = " + query);
		try (PreparedStatement ps = conn.prepareStatement(query.toString(), Statement.RETURN_GENERATED_KEYS)) {
			ps.setObject(1, rowClassInfo.primaryKeyField.get(row));
			if (1 != ps.executeUpdate()) {
				throw new SQLException("Failed:  " + query.toString());
			}
			row.setInDb(false);
		} finally {
			close();
		}
	}

	private String getRowQuerySQL() {
		if (rowQuerySQL != null) {
			log.info("!!!!!!!! rowQuerySQL = " + rowQuerySQL);
			return rowQuerySQL;
		}

		if (rowClassInfo != null) {
			return rowClassInfo.querySQL;
		} else {
			rowClassInfo = rowClassInfoCache.get(rowClass);
			if (rowClassInfo == null) {
				rowClassInfo = new RowClassInfo();
				rowClassInfoCache.put(rowClass, rowClassInfo);

				rowClassInfo.tableName = camelToUnderscore(rowClass.getSimpleName());

				Select select = rowClass.getAnnotation(Select.class);
				/*
				 * if (select == null) { throw new IllegalArgumentException(
				 * "SQL.allResults() requires XxxRow class have a @Select(query=\"select ...\")"
				 * ); }
				 */
				if (select == null) {
					StringBuilder sb = new StringBuilder("select ");
					String comma = "";
					for (Field field : rowClass.getFields()) {
						String fieldName = field.getName();
						if (fieldName.equals("rowNum")) {
							continue;
						}
						sb.append(comma);
						comma = ", ";
						sb.append(camelToUnderscore(fieldName));
					}
					sb.append(" from ");
					sb.append(rowClassInfo.tableName);
					rowClassInfo.querySQL = sb.toString();
					log.info("Generated: " + rowClassInfo.querySQL);
				} else {
					rowClassInfo.querySQL = select.value();
				}
			}
			rowQuerySQL = rowClassInfo.querySQL;
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

	/**
	 * Convert userId to user_id, AbcDEFghi to abc_defghi. Replace all lU with
	 * l_u. Also strips trailing "Row" to convert class name UserRow to table
	 * name user.
	 * 
	 * @param camel-case
	 *            string
	 * @return string with camel transitions replaced by underscores
	 */
	public static String camelToUnderscore(String camel) {
		return camel.replaceAll("Row$", "").replaceAll("(\\p{Lower})(\\p{Upper})", "$1_$2").toLowerCase();
	}

	/**
	 * Convert user_id to userId.
	 * 
	 * @param und
	 *            string
	 * @return string with underscores replaced by camel back.
	 */
	public static String underscoreToCamel(String und, boolean isClassName) {
		// return und.replaceAll("(.)_(.)", "$1$2");
		StringBuilder sb = new StringBuilder();
		boolean isFirstWord = true;
		for (String word : und.toLowerCase().split("_")) {
			String firstChar = word.substring(0, 1);
			sb.append(isFirstWord && !isClassName ? firstChar : firstChar.toUpperCase());
			sb.append(word.substring(1));
			isFirstWord = false;
		}
		return sb.toString();
	}

}
