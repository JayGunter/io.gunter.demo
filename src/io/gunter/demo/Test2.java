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
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException {
		EmployeeRow empRow = new EmployeeRow();
		empRow.firstName = "f1";
		empRow.lastName = "l1";
		empRow.save(getConn());
		log.info("id=" + empRow.id + ", rowNum=" + empRow.rowNum + ", fromDb=" + empRow.inDb());
		empRow.firstName = "uf1";
		empRow.save(getConn());
		empRow.delete(getConn());
		
		empRow = EmployeeRow.getById(EmployeeRow.class, empRow.id, getConn());
		log.info("got row id = " + empRow.id);
	}

}
