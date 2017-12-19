/**
 * 
 */
package io.gunter.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import lombok.extern.slf4j.Slf4j;

/**
 * @author jay
 *
 */
@Slf4j
public class Test2 {
	
	public static Connection getConn() throws SQLException {
		return DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb", "root", "");
	}


	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 * @throws IllegalArgumentException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IllegalAccessException, SQLException {
		EmployeeRow empRow = new EmployeeRow();
		empRow.firstName = "f1";
		empRow.lastName = "l1";
		empRow.save(getConn());
		log.info("id=" + empRow.id);
	}

}
