package com.trivago.dataegnineering.hive.udf.collectionhelper;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.trivago.dataegnineering.hive.udf.collectionhelper.testutils.HiveRowTestUtil;
import com.trivago.dataegnineering.hive.udf.collectionhelper.testutils.HiveUDFTestUtil;

public class ReduceCollectionTest {

	
	static {
		HiveUDFTestUtil.registerUdfs(ConcatenateAllMap.class,ListToMapExtraArgs.class,ListToMapExtraArgsExpectInited.class,
				TakeWriteableReturnInt.class,IreturnTheWrongType.class);
	}
	
	public static class ConcatenateAllMap extends UDF {
		
		public String evaluate(String mapKey, String mapVal, String oldAgg) {
			return oldAgg + ",[" + mapKey + "=>" + mapVal + "]";
			
		}
		
	}

	private GenericUDF udf;
	
	@Before
	public void setUp() {
		udf = new ReduceCollection();
	}
	
	@Test
	public void testMapToStringInit() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("ConcatenateAllMap")),
				ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
																		PrimitiveObjectInspectorFactory.javaStringObjectInspector),
				PrimitiveObjectInspectorFactory.javaStringObjectInspector
		});
		String ret = (String) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("ConcatenateAllMap"), HiveRowTestUtil.stringMap(),"beginning"));
		assertTrue(ret.length() > 10);
		assertTrue(ret.startsWith("beginning"));
	}
	
	@Test
	public void testMapToStringlongNoInitInit() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("ConcatenateAllMap")),
				ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
																	PrimitiveObjectInspectorFactory.javaStringObjectInspector),
				PrimitiveObjectInspectorFactory.javaStringObjectInspector,
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.booleanTypeInfo, new BooleanWritable(false))
		});
	String ret = (String) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("ConcatenateAllMap"), HiveRowTestUtil.stringMap(),"beginning", false));
	assertTrue(ret.length() > 10);
	assertTrue(ret.startsWith("null"));
	}
	
	public static class ListToMapExtraArgs extends UDF{
		
		boolean sawNullMap = false;
		
		public Map<String,Integer> evaluate(String listElem, Map<String,Integer> carryOn, boolean extraArg, short extraArg2 ){
			if(!sawNullMap && carryOn == null) {
				sawNullMap = true;
				carryOn = new HashMap<String,Integer>();
			}
			assertTrue(sawNullMap && extraArg);
			assertEquals(extraArg2, 2);
			carryOn.put(listElem, listElem.length() * extraArg2);
			return carryOn;
		}
	}
	
	@Test
	public void testListToMapNoInitTwoExtraArgs() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("ListToMapExtraArgs")),
				ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
				ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
																	PrimitiveObjectInspectorFactory.javaIntObjectInspector),
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.booleanTypeInfo, new BooleanWritable(false)),
				PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
				PrimitiveObjectInspectorFactory.javaShortObjectInspector
		});
	Map<String,Integer> ret = (Map<String,Integer>) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("ListToMapExtraArgs"), HiveRowTestUtil.stringList(),null, false,true, (short) 2));
	assertNotNull(ret);
	assertEquals(ret.get("bbbb"),(Integer)8);
	
	}
	
	
public static class ListToMapExtraArgsExpectInited extends UDF{

		
		public Map<String,Integer> evaluate(String listElem, Map<String,Integer> carryOn, boolean extraArg, short extraArg2 ){
			assertNotNull(carryOn);
			assertTrue(extraArg);
			assertEquals(extraArg2, 2);
			carryOn.put(listElem, listElem.length() * extraArg2);
			return carryOn;
		}
	}
	
	@Test
	public void testListToMapInitTwoExtraArgs() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("ListToMapExtraArgsExpectInited")),
				ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
				ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
																	PrimitiveObjectInspectorFactory.javaIntObjectInspector),
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.booleanTypeInfo, new BooleanWritable(true)),
				PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
				PrimitiveObjectInspectorFactory.javaShortObjectInspector
		});
		HashMap<String,Integer> prepopulated = new HashMap<>();
		prepopulated.put("Already", 18);
		Map<String,Integer> ret = (Map<String,Integer>) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("ListToMapExtraArgsExpectInited"),
				HiveRowTestUtil.stringList(), prepopulated, true, true, (short) 2));
		assertTrue(ret != null);
		assertEquals(ret.get("bbbb"), (Integer)8);
		assertEquals(ret.get("Already"), (Integer) 18);
	
	}
	
	public static class TakeWriteableReturnInt extends UDF {
		
		public Integer evaluate(String element, Integer carry, boolean dummy, short dummy2) {
			return carry + 1;
		}
		
	}
	
	@Test
	public void testMixedTypesNotConfused() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("TakeWriteableReturnInt")),
				ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
				PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.intTypeInfo),
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.booleanTypeInfo, new BooleanWritable(true)),
				PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
				PrimitiveObjectInspectorFactory.javaShortObjectInspector
		});
		
		Integer ret = (Integer) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("TakeWriteableReturnInt"),
				HiveRowTestUtil.stringList(), new IntWritable(17), true, true, (short) 2));
		assertNotNull(ret);
	}
	
	
	public static class IreturnTheWrongType extends UDF {
		
		public Map<String,IreturnTheWrongType> evaluate(String element, Integer carry, boolean dummy, short dummy2) {
			return new HashMap<>();
		}
		
	}
	
	@Test (expected = UDFArgumentException.class)
	public void blowUpOnWrongUDFReturnType() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("IreturnTheWrongType")),
				ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
				PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.intTypeInfo),
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.booleanTypeInfo, new BooleanWritable(true)),
				PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
				PrimitiveObjectInspectorFactory.javaShortObjectInspector
		});
	
	
	}

}
