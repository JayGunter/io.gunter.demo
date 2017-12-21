package io.gunter.demo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;

public abstract class Row<RowClass extends Row> {
	public Integer rowNum;
	private boolean dirty;
	private boolean inDb;

	public boolean inDb() {
		return inDb;
	}

	public void setInDb(boolean b) {
		inDb = true;
	}

	/**
	 * Usage: userRow.mod(); userRow.age++; userRow.save();
	 * 
	 * @return
	 */
	public Row mod() {
		dirty = true;
		return this;
	}

	// TODO would this be useful? It doesn't help static methods
	// that don't receive an object/class to work with.
	public RowClass getSubTypedObject() {
		return (RowClass) this;
	}

	/*
	 * public static Row where(Object... keysAndValues) { return new
	 * SQL<this.class>(); }
	 */

	// TODO could this work to return a stream of e.g. UserRow objects?
	public Stream<Row> stream(Connection conn) {
		return null;
	}

	public static <RowClass extends Row> RowClass getById(Class rowClazz, Integer id, Connection conn)
			throws InstantiationException, IllegalAccessException, SQLException {
		SQL<RowClass> sql = new SQL<>(rowClazz, conn);
		return (RowClass) sql.getById(rowClazz, id, conn);
	}

	public void save(Connection conn) throws IllegalArgumentException, IllegalAccessException, SQLException {
		try {
			SQL<Row> sql = new SQL<>(this, conn);
			if (this.inDb) {
				sql.update();
			} else {
				sql.insert();
			}
		} finally {
			conn.close();
		}
	}
	
	public void delete(Connection conn) throws IllegalArgumentException, IllegalAccessException, SQLException {
		try (SQL<Row> sql = new SQL<>(this, conn)) {
			if (this.inDb) {
				sql.delete();
			} else {
				throw new IllegalArgumentException("Row id=" + sql.getPkCamelName() + " = " + sql.getPkValue());
			}
		} finally {
			conn.close();
		}
	}

}
