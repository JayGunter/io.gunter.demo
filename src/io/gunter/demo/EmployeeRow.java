package io.gunter.demo;

/**
 * EmployeeRow represents a row in the EMPLOYEE table. The SQL class drops the
 * "Row" to get the table name.
 * 
 * Each public field corresponds to a column in EMPLOYEE. Camel-case names are
 * mapped to underscored names. So the firstName field maps to the FIRST_NAME
 * column.
 * 
 * A column named id is assumed to be the primary key column. If the primary key
 * column is not ID, use the @Pk annotation to mark the corresponding field.
 * 
 * @author jaygunter
 *
 */
public class EmployeeRow extends Row {
	public Integer id;
	public String firstName;
	public String lastName;
	public Integer mgrId;
}
