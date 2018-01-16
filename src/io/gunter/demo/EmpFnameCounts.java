package io.gunter.demo;

import io.gunter.demo.SQL.Select;

import static java.lang.System.out;

import io.gunter.demo.SQL.Order;

@Select("select first_name as fname, count(*) as tally from employee where id > ? group by fname")
public class EmpFnameCounts extends Row<EmpFnameCounts> {

	@Order(value = 2)
	public Integer count;
	public String fname;

	public void action() {
		out.println("row#" + rowNum + ": fname=" + fname + ", count: " + count);
	}
}
