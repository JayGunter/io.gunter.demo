package io.gunter.demo;

import static java.lang.System.out;

public class UserRow extends Row {

	/*
	public UserRow() {
	}
	*/

	/*
	// @Override
	public static String getQuery() {
		return "select username, email, age from user";
	}
	*/
	public static final String query = "select username, email, age from user";

	public Integer age;
	public String name;
	public String mail;

	// optional 'action' will be used by SQL.run(RowObj,Connection)
	public void action() {
		out.println("row#" + rowNum + ": name=" + name + ", age: " + age + ", email: " + mail);
	}

	// alternate action for use with
	// SQL.run(RowObj,Connection,Consumer<RowClass>)
	public void action2() {
		out.println("ACTION2: row#" + rowNum + ": name=" + name + ", age: " + age + ", email: " + mail);
	}

}
