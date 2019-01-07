package com.trivago.dataegnineering.hive.udf.collectionhelper;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
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

public class FilterCollectionTest {

	
	private GenericUDF udf;
	
	static {
		HiveUDFTestUtil.registerUdfs(InString.class,IsSquared.class,MapValueIsAddedAsString.class,SimpleList.class);
	}
	
	@Before
	public void setUp() {
		udf = new FilterCollection();
	}
	
	
	public static class SimpleList extends UDF {
		
		public boolean evaluate(String element) {
			return element.length() > 4;
		}
	}
	
	
	@Test
	public void simpleListTest () throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("SimpleList")),
				ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
				});
		List ret = (List) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("InString"), HiveRowTestUtil.stringList()));
		assertEquals(ret.size(),3);
		
	}
	
	public static class InString extends UDF {
		
		public boolean evaluate(String arrayElem, String test) {
			return test.contains(arrayElem);
		}
		
	}

	
	@Test
	public void testFilterListWithExtraArg() throws HiveException {
		
		udf.initialize(new ObjectInspector[] {
								PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("InString")),
								ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
								PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("aaa/bbb/cccc"))});
		List ret = (List) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("InString"), HiveRowTestUtil.stringList(), new Text("aaa/bbbb/cccccc")));
		assertEquals(ret.size(),3);
	}
	
	public static class IsSquared extends UDF {
			
		public boolean evaluate(Integer mapKey, Integer mapValue) {
			return mapKey * mapKey == mapValue;
		}
		
	}
	
	@Test
	public void testMap() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("IsSquared")),
				ObjectInspectorFactory.getStandardMapObjectInspector(
										PrimitiveObjectInspectorFactory.javaIntObjectInspector,
										PrimitiveObjectInspectorFactory.javaIntObjectInspector)
				});
		Map ret = (Map) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("IsSquared"), HiveRowTestUtil.intMap()));
		assertEquals(ret.size(),2);
		assertEquals(ret.get(3),9);
		assertNull(ret.get(2));
	}
	
	
	public static class MapValueIsAddedAsString extends UDF {
		
		public boolean evaluate(Integer mapKey, Integer mapValue, String expected) {
			return expected.equals("" +(mapValue + mapKey));
		}
		
	}
	
	
	@Test
	public void testMapWithExtraArgs() throws HiveException {
		udf.initialize(new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text("MapValueIsAddedAsString")),
				ObjectInspectorFactory.getStandardMapObjectInspector(
										PrimitiveObjectInspectorFactory.javaIntObjectInspector,
										PrimitiveObjectInspectorFactory.javaIntObjectInspector),
				PrimitiveObjectInspectorFactory.javaStringObjectInspector
				});
		Map ret = (Map) udf.evaluate(HiveRowTestUtil.getDeferredVersion(new Text("MapValueIsAddedAsString"), HiveRowTestUtil.intMap(),"20"));
		assertEquals(ret.size(),1);
		assertEquals(ret.get(4),16);
		assertNull(ret.get(2));
	}

}
