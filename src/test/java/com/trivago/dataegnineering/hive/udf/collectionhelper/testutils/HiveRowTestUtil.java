package com.trivago.dataegnineering.hive.udf.collectionhelper.testutils;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

public class HiveRowTestUtil {
	
	public static List<String> stringList(){
		LinkedList<String> stringList = new LinkedList<>();
		stringList.add("aaa");
		stringList.add("bbbb");
		stringList.add("ccccc");
		stringList.add("dddddd");
		stringList.add("fffffff");
		return stringList;
		
	}
	
	public static Map<Integer,Integer> intMap(){
		HashMap<Integer,Integer> intMap = new HashMap<>();
		intMap.put(3, 9);
		intMap.put(4, 16);
		intMap.put(2,5);
		return intMap;
	}
	
	public static Map<String,String> stringMap() {
		Map<String, String> stringMap = new HashMap<>();
		stringMap.put("aaa","bbb");
		stringMap.put("aaaa","bbb");
		return stringMap;
		
	}
	
	public static DeferredObject[] getDeferredVersion(Object ... args) {
		DeferredObject[] def = new DeferredObject[args.length];
		for(int i = 0 ; i < args.length ; i++) {
			def[i] =  new DeferredJavaObject(args[i]);
		}
		return def;
	}
	
	public static void registerUdfs(Class ... args) {
		
	}
}
