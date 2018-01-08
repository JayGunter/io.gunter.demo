package io.gunter.demo;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import io.gunter.demo.SQL.PK;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Row<RowClass extends Row<?>> {
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
	public Row<RowClass> mod() {
		dirty = true;
		return this;
	}

	public boolean isMod() {
		return dirty;
	}

	// TODO would this be useful? It doesn't help static methods
	// that don't receive an object/class to work with.
	@SuppressWarnings("unchecked")
	public RowClass getSubTypedObject() {
		return (RowClass) this;
	}

	/*
	 * public static Row where(Object... keysAndValues) { return new
	 * SQL<this.class>(); }
	 */

	// TODO could this work to return a stream of e.g. UserRow objects?
	public Stream<RowClass> stream(Connection conn) {
		return null;
	}

	public static <RowClass extends Row<?>> RowClass getById(Class<RowClass> rowClazz, Integer id, Connection conn)
			throws InstantiationException, IllegalAccessException, SQLException {
		return SQL.<RowClass>getById(rowClazz, id, conn);
		/*
		 * try (SQL<RowClass> sql = new SQL<>(rowClazz, conn)) { return
		 * (RowClass) sql.getById(rowClazz, id, conn); }
		 */
	}

	public static <RowClass extends Row<?>> List<RowClass> query(Connection conn, Class<RowClass> clazz,
			String whereClause, Object... whereValues)
			throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException {
		try (SQL<RowClass> sql = new SQL<RowClass>(clazz, conn)) {
			return sql.query(whereClause, whereValues);
		} finally {
			conn.close();
		}
	}

	public void save(Connection conn) throws IllegalArgumentException, IllegalAccessException, SQLException {
		try (@SuppressWarnings("unchecked")
		SQL<RowClass> sql = new SQL<RowClass>((RowClass) this, conn)) {
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
		try (@SuppressWarnings("unchecked")
		SQL<RowClass> sql = new SQL<RowClass>((RowClass) this, conn)) {
			if (this.inDb) {
				sql.delete();
			} else {
				throw new IllegalArgumentException("Row id=" + sql.getPkCamelName() + " = " + sql.getPkValue());
			}
		} finally {
			conn.close();
		}
	}

	/**
	 * subclasses of Row can use this implementation
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(this.getClass().getName());
		sb.append(":");
		String comma = "";
		for (Field field : this.getClass().getFields()) {
			sb.append(comma);
			comma = ",";
			sb.append(field.getName());
			sb.append("=");
			try {
				sb.append(field.get(this));
			} catch (IllegalArgumentException | IllegalAccessException e) {
				sb.append("unknown");
			}
		}
		return sb.toString();
	}

	/**
	 * subclasses of Row can use this implementation
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (dirty ? 1231 : 1237);
		result = prime * result + (inDb ? 1231 : 1237);
		result = prime * result + ((rowNum == null) ? 0 : rowNum.hashCode());
		for (Field field : this.getClass().getFields()) {
			try {
				Object obj = field.get(this);
				result = prime * result + ((obj == null) ? 0 : obj.hashCode());
			} catch (IllegalArgumentException | IllegalAccessException e) {
			}
		}
		return result;
	}

	/**
	 * subclasses of Row can use this implementation
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		@SuppressWarnings("unchecked")
		Row<RowClass> other = (Row<RowClass>) obj;
		if (dirty != other.dirty)
			return false;
		if (inDb != other.inDb)
			return false;
		if (rowNum == null) {
			if (other.rowNum != null)
				return false;
		} else if (!rowNum.equals(other.rowNum))
			return false;
		for (Field field : this.getClass().getFields()) {
			try {
				Object valThis = field.get(this);
				Object valOther = field.get(other);
				if (valThis == null && valOther != null)
					return false;
				if (valThis != null && valOther == null)
					return false;
				if (!valThis.equals(valOther))
					return false;
			} catch (IllegalArgumentException | IllegalAccessException e) {
			}
		}
		return true;
	}

	/**
	 * Set all fields excluding 'id' or field marked @Pk.
	 * @param values
	 * @return this
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	@SuppressWarnings("unchecked")
	public RowClass init(Object... values) throws IllegalArgumentException, IllegalAccessException {
		Field[] allFields = this.getClass().getFields();
		List<Field> fields = new ArrayList<>(allFields.length);
		for (Field field : allFields) {
			if (! (field.getName().equals("id") || field.isAnnotationPresent(PK.class))) {
				fields.add(field);
			}
		}
		if (fields.size() < values.length) {
			throw new IllegalArgumentException("More values (" + values.length + ") than fields (" + fields.size());
		}
		int i = 0;
		for (Object value : values) {
			fields.get(i++).set(this, value);
		}
		return (RowClass) this;
	}

	/**
	 * Set all fields including 'id' or field marked @Pk.
	 * @param values
	 * @return this
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	@SuppressWarnings("unchecked")
	public RowClass set(Object... values) throws IllegalArgumentException, IllegalAccessException {
		Field[] fields = this.getClass().getFields();
		if (fields.length < values.length) {
			throw new IllegalArgumentException("More values (" + values.length + ") than fields (" + fields.length);
		}
		int i = 0;
		for (Object value : values) {
			fields[i++].set(this, value);
		}
		return (RowClass) this;
	}

}
