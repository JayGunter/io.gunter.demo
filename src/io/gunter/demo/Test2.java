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
	 * @throws SecurityException 
	 * @throws NoSuchMethodException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, SecurityException {
		EmployeeRow empRow = new EmployeeRow();
		empRow.firstName = "f1";
		empRow.lastName = "l1";
		empRow.save(getConn());
		log.info("id=" + empRow.id + ", rowNum=" + empRow.rowNum + ", fromDb=" + empRow.inDb());
		empRow.firstName = "uf1";
		empRow.save(getConn());
		empRow.delete(getConn());
		
		empRow = EmployeeRow.getById(EmployeeRow.class, empRow.id, getConn());
		if (empRow != null) {
			log.error("Delete failed. Got row id = " + empRow.id);
		} else {
			log.info("Delete succeeded.");
		}
		empRow = EmployeeRow.getById(EmployeeRow.class, 222, getConn());
		log.info("got row id = " + empRow.id);
		
		log.info("-------------");
		//SQL.run(EmpFnameCounts.class, getConn()); 
		//EmpFnameCounts fnameCounts = new EmpFnameCounts fnameCounts = new 
		SQL.query(EmpFnameCounts.class, getConn()).whereValues(233).run(); 
	}

}
