package io.gunter.demo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;

public abstract class Row {
	public Integer rowNum;
	private boolean dirty;

	/**
	 * Usage:  userRow.mod(); userRow.age++; userRow.save();
	 * @return
	 */
	public Row mod() {
		dirty = true;
		return this;
	}

	/*
	public static Row where(Object... keysAndValues) {
		return new SQL<this.class>();
	}
	*/
	
	// TODO could this work to return a stream of e.g. UserRow objects?
	public Stream<Row> stream(Connection conn) {
		return null;
	}

	/* TODO bag this in favor of insert/update */
	public void save(Connection conn) throws IllegalArgumentException, IllegalAccessException, SQLException {
		try {
			SQL<Row> sql = new SQL<>(this, conn);
			sql.insert();
		} finally {
			conn.close();
		}

	}

//	public static String getQuery() { return null; }
}
