package com.trivago.dataegnineering.hive.udf.collectionhelper;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.trivago.dataegnineering.hive.udf.collectionhelper.testutils.HiveRowTestUtil;
import com.trivago.dataegnineering.hive.udf.collectionhelper.testutils.HiveUDFTestUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class MapCollectionTest {

	
	static {
		HiveUDFTestUtil.registerUdfs(MapToKeyValue.class, MapToString.class, MapMultipleToString.class,
				ReverseMap.class,MapToKeyValueExtraArgs.class);
	}

	private GenericUDF udf;
	
	@Before
	public void setUp() {
		udf = new MapCollection();
	}
	
	public static class KeyValue {
		
		//visible for reflection Object Inspector
		public KeyValue() {};
		
		@SuppressFBWarnings
		public String key,value;
	
		public KeyValue(String key, String value) {
			this.key = key; this.value = value;
		}
	}
	
	public static class MapToKeyValue extends UDF {
		
		public KeyValue evaluate (String arrayElem) {
			return new KeyValue(new String(new char[] {arrayElem.charAt(0)}),arrayElem);
		}
		
	}
	
	public static class MapToString extends UDF {
		
		public String evaluate (String arrayElem) {
			return new String(new char[] {arrayElem.charAt(0)});
		}
	}
	
	public static class MapMultipleToString extends UDF {
		
		public String evaluate (String mapkey, String mapValue) {
			return mapkey + "-" +mapValue;
		}
	}
	
	public static class ReverseMap extends UDF {
		
		public KeyValue evaluate (String mapkey, String mapValue) {
			return new KeyValue(mapValue, mapkey);
		}
	}
	
	public static class MapToKeyValueExtraArgs extends UDF {
		
		public KeyValue evaluate (String arrayElem, Integer intParam, Map<Integer,Boolean> mapParam) {
			assertEquals(intParam, (Integer) 14);
			assertNotNull(mapParam);
			assertEquals(mapParam.size(), 0);
			return new KeyValue(new String(new char[] {arrayElem.charAt(0)}),arrayElem);
		}
		
	}
	
	
	@Test
	public void testMapToMapWithoutExtraArgs() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("ReverseMap")),
				ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
																		PrimitiveObjectInspectorFactory.javaStringObjectInspector)
		});
		Map ret = (Map) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("ReverseMap"), HiveRowTestUtil.stringMap()));
		assertEquals(1,ret.size());
		assertNotNull(ret.get("bbb"));
		
	}
	
	@Test
	public void testMapToListWithoutExtraArgs() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("MapMultipleToString")),
				ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
																		PrimitiveObjectInspectorFactory.javaStringObjectInspector)
		});
		List ret = (List) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("MapMultipleToString"), HiveRowTestUtil.stringMap()));
		assertEquals(HiveRowTestUtil.stringMap().size(),ret.size());
		assertTrue(ret.contains("aaaa-bbb"));
	}
	
	
	@Test
	public void testListToListWithoutExtraArgs() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("MapToString")),
				ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
		});
		List ret = (List) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("MapToString"), HiveRowTestUtil.stringList()));
		assertEquals(HiveRowTestUtil.stringList().size(),ret.size());
	}
	
	
	@Test
	public void testListToMapWithoutExtraArgs() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("MapToKeyValue")),
				ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
		});
		Map ret = (Map) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("MapToKeyValue"), HiveRowTestUtil.stringList()));
		assertEquals(HiveRowTestUtil.stringList().size(),ret.size());
	}

	
	
	@Test
	public void testListToMapWithExtraArgs() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("MapToKeyValueExtraArgs")),
				ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
				PrimitiveObjectInspectorFactory.javaIntObjectInspector,
				ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
																	PrimitiveObjectInspectorFactory.javaBooleanObjectInspector)
		});
		Map ret = (Map) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("MapToKeyValueExtraArgs"), HiveRowTestUtil.stringList(),14,new HashMap<>()));
		assertEquals(HiveRowTestUtil.stringList().size(),ret.size());
	}
	
	@Test (expected = UDFArgumentException.class)
	public void testArgumentException () throws UDFArgumentException {
		
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("MapToKeyValueExtraArgs")),
				ObjectInspectorFactory.getStandardStructObjectInspector(Collections.singletonList("aa"), Collections.singletonList(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector))
		});
	}

	
}
