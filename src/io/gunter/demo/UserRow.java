package io.gunter.demo;

import static java.lang.System.out;

import io.gunter.demo.SQL.Order;
import io.gunter.demo.SQL.Select;

@Select(query="select user_name, email, age, password from user")
public class UserRow extends Row {

	public Integer age;
	public String userName;
	public String mail;
	public String password;

	// optional 'action' will be used by SQL.run(RowObj,Connection)
	public void action() {
		out.println("row#" + rowNum + ": userName=" + userName + ", age: " + age + ", email: " + mail);
	}

	// alternate action for use with
	// SQL.run(RowObj,Connection,Consumer<RowClass>)
	public void action2() {
		out.println("ACTION2: row#" + rowNum + ": userName=" + userName + ", age: " + age + ", email: " + mail);
	}

}
