package com.trivago.dataegnineering.hive.udf.collectionhelper.testutils;

import java.lang.reflect.Field;

import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveUDFTestUtil {
	
	public static void registerUdfs(Class<? extends UDF> ... args) {
		try {
			Field f = FunctionRegistry.class.getDeclaredField("system");
			f.setAccessible(true);
			Registry r = (Registry) f.get(null);
			for(Class<? extends UDF> c : args) {
				r.registerUDF(c.getSimpleName(), c, false, (FunctionResource) null);
			}
			
			
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
