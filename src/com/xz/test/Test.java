package com.xz.test;

import java.math.BigDecimal;

public class Test {
	public static void main(String[] args) {
		BigDecimal bigDecimal1 = new BigDecimal(5) ;
		BigDecimal bigDecimal2 = new BigDecimal(1) ;
		bigDecimal1 = bigDecimal1.add(bigDecimal2) ;
		System.out.println(bigDecimal1.intValue());
	}
}
