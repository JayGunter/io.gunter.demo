/**
 * 
 */
package io.gunter.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

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
		Test2 test2 = new Test2();
		test2.go();
	}
	
	private void go() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, SecurityException {
		EmployeeRow empRow = new EmployeeRow();
		empRow.firstName = "f1";
		empRow.lastName = "l1";
		empRow.save(getConn());
		log.info("id=" + empRow.id + ", rowNum=" + empRow.rowNum + ", fromDb=" + empRow.inDb());

		empRow.firstName = "uf1";
		empRow.save(getConn());
		empRow = EmployeeRow.getById(EmployeeRow.class, empRow.id, getConn());
		log.info("got row id = " + empRow.id + ", version = " + empRow.version);
		
		// TODO do not pass select
		List<EmployeeRow> emps = Row.query(getConn(), EmployeeRow.class, "first_name = ?", "uf1");
		log.info("got " + emps.size() + " emps");
		for (EmployeeRow emp : emps) {
			log.info("id=" + emp.id);
		}

		int delId = empRow.id;
		empRow.delete(getConn());
		empRow = EmployeeRow.getById(EmployeeRow.class, delId, getConn());
		if (null == empRow) {
			log.info("Deleted row id = " + delId);
		} else {
			log.info("Failed to delete row id = " + empRow.id);
		}
		
		log.info("-------------");
		log.info("--- run");
		int minId = 10;
		//SQL.run(EmpFnameCounts.class, getConn()); 
		//EmpFnameCounts fnameCounts = new EmpFnameCounts fnameCounts = new 
		SQL.query(EmpFnameCounts.class, getConn()).whereValues(minId).run(); 
		log.info("--- forEach row::action");
		SQL.query(EmpFnameCounts.class, getConn()).whereValues(minId).forEach(EmpFnameCounts::action);
		log.info("--- forEach lambda");
		SQL.query(EmpFnameCounts.class, getConn()).whereValues(minId).forEach((EmpFnameCounts r) -> s += " " + r.count);
		log.info("s = " + s);
		log.info("--- forEach other::action");
		SQL.query(EmpFnameCounts.class, getConn()).whereValues(minId).forEach(Test2::action2);
		log.info("--- allResults");
		List<EmpFnameCounts> efc = SQL.query(EmpFnameCounts.class, getConn()).whereValues(minId).allResults();
		log.info("got " + efc.size() + " rows");
		log.info("--- loop");
		EmpFnameCounts row = null;
		SQL<EmpFnameCounts> sql = SQL.query(EmpFnameCounts.class, getConn()).whereValues(minId);
		while (null != (row = sql.query())) {
			System.out.println("loop row.fname = " + row.fname);
		}
		
		log.info("################# END ################");

	}

	public static void action2(EmpFnameCounts row) {
		System.out.println("fname=" + row.fname + ", tally=" + row.count);
	}

		String s = "counts: ";
}
