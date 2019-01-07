package com.trivago.dataegnineering.hive.udf.collectionhelper;

import static org.junit.Assert.*;

import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import com.trivago.dataegnineering.hive.udf.collectionhelper.testutils.HiveRowTestUtil;

public class UpdateCollectionTest {

	private UpdateCollection udf;


	@Before
	public void setUp() {
		udf = new UpdateCollection();
	}
	
	
	@Test
	public void testListTypeConvertedProperly() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				ObjectInspectorFactory.getStandardListObjectInspector(
							PrimitiveObjectInspectorFactory.writableIntObjectInspector),
				PrimitiveObjectInspectorFactory.javaIntObjectInspector
		});
		List l = (List) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new LinkedList(),15));
		IntWritable iw = (IntWritable) l.get(0);
		assertEquals(iw, new IntWritable(15));
	}
	
	@Test
	public void testMapTypeConvertedProperly() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				ObjectInspectorFactory.getStandardMapObjectInspector(
							PrimitiveObjectInspectorFactory.writableIntObjectInspector, 
							PrimitiveObjectInspectorFactory.javaBooleanObjectInspector),
				PrimitiveObjectInspectorFactory.javaIntObjectInspector,
				PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
		});
		Map l = (Map) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new HashMap(),15, new BooleanWritable(false)));
		Boolean bl = (Boolean) l.get(new IntWritable(15));
		assertFalse(bl);
	}
	
	@Test(expected = UDFArgumentException.class)
	public void testThrowOnTypeFUp() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				ObjectInspectorFactory.getStandardMapObjectInspector(
							PrimitiveObjectInspectorFactory.writableIntObjectInspector, 
							PrimitiveObjectInspectorFactory.javaBooleanObjectInspector),
				PrimitiveObjectInspectorFactory.javaStringObjectInspector,
				PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
		});
	}
	
	@Test 
	public void testHandleTypeConversion() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				ObjectInspectorFactory.getStandardMapObjectInspector(
							PrimitiveObjectInspectorFactory.writableIntObjectInspector, 
							PrimitiveObjectInspectorFactory.javaBooleanObjectInspector),
				PrimitiveObjectInspectorFactory.javaIntObjectInspector,
				PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
		});
		
		Map<?, ?> mapToUpdate = new HashMap<>();
		udf.evaluate(HiveRowTestUtil.getDeferredVersion(mapToUpdate, 17,new BooleanWritable(false)));
		assertNotNull(mapToUpdate.get(new IntWritable(17)) );
		assertNotNull(mapToUpdate.get(new IntWritable(17)).getClass().equals(Boolean.class));
	}

}
