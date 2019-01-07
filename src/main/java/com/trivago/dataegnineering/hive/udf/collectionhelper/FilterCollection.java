package com.trivago.dataegnineering.hive.udf.collectionhelper;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;

import com.trivago.dataegnineering.hive.udf.collectionhelper.utils.GenericUDFParamBridge;

public class FilterCollection extends GenericUDF implements Serializable {
	
	transient private static final long serialVersionUID = 1L;
	private GenericUDF udf;
	transient private BooleanObjectInspector makroBooleanOi;
	transient private DeferredObject[] paramWrapper;
	transient private MapObjectInspector mapOi;
	transient private ListObjectInspector listOi;
	transient private StandardMapObjectInspector mapOutputOi;
	transient private StandardListObjectInspector listOutputOi;
	transient private GenericUDFParamBridge bridge;
	

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		String functionName = getConstantStringValue(arguments, 0);
		if(udf == null) { 
			FunctionInfo fi;
			try {
				fi = FunctionRegistry.getFunctionInfo(functionName);
				udf = fi.getGenericUDF();
				udf.getClass();
			} catch (Exception  e) {
				throw new UDFArgumentException(e);
			}
			
		}
		
		ObjectInspector makroReturnOi;
		if(arguments[1].getCategory() == ObjectInspector.Category.LIST) {
			
			listOi = (ListObjectInspector) arguments[1];
			ObjectInspector[] allArgs = new ObjectInspector[1 + arguments.length -2]; // (list entry) + (plus all the arguments) - (string and map)
			allArgs[0] = listOi.getListElementObjectInspector();
			paramWrapper = new DeferredJavaObject[allArgs.length];
			System.arraycopy(arguments, 2, allArgs, 1, arguments.length - 2);
			bridge = GenericUDFParamBridge.getParameterInspectors(udf, allArgs);
			makroReturnOi = udf.initialize(bridge.getOis());
			listOutputOi =  ObjectInspectorFactory.getStandardListObjectInspector(listOi.getListElementObjectInspector());
			
		} else if(arguments[1].getCategory() == ObjectInspector.Category.MAP)
		{
			paramWrapper = new DeferredJavaObject[3];
			mapOi = (MapObjectInspector) arguments[1];
			ObjectInspector[] allArgs = new ObjectInspector[2 + arguments.length -2]; // (map key  and value) + (plus all the arguments) - (string and map)
			paramWrapper = new DeferredJavaObject[allArgs.length];
			allArgs[0] = mapOi.getMapKeyObjectInspector();
			allArgs[1] = mapOi.getMapValueObjectInspector();
			System.arraycopy(arguments, 2, allArgs, 2, arguments.length -2);
			bridge = GenericUDFParamBridge.getParameterInspectors(udf, allArgs);
			makroReturnOi = udf.initialize(bridge.getOis());
			mapOutputOi =  ObjectInspectorFactory.getStandardMapObjectInspector(mapOi.getMapKeyObjectInspector(),mapOi.getMapValueObjectInspector());
		}else {
			throw new UDFArgumentException("second param needs to be a list or map");
		}
		
		if(makroReturnOi.getCategory() != ObjectInspector.Category.PRIMITIVE ||
				((PrimitiveObjectInspector) makroReturnOi).getPrimitiveCategory() 
				!= PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
			throw new UDFArgumentException("function needs to return boolean");
		}

		makroBooleanOi = (BooleanObjectInspector) makroReturnOi;
	
		return listOutputOi == null ? mapOutputOi : listOutputOi;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if( listOutputOi != null) {
			LinkedList<Object> output = new LinkedList<>();
			List<?> l =  listOi.getList(arguments[1].get());
			if(l == null) {
				return null;
			}
			for(Object o : l) {
				paramWrapper[0] = new DeferredJavaObject(o);
				System.arraycopy(arguments, 2, paramWrapper, 1, arguments.length -2);
				Object booleanTest = udf.evaluate(bridge.deferr(paramWrapper));
				if(makroBooleanOi.get(booleanTest)) {
					output.add(o);
				}
			}
			Object OiList =  listOutputOi.create(output.size());
			for(int i = 0;i < output.size() ; i++) {
				listOutputOi.set(OiList, i, output.get(i));
			}
			return OiList;
		} else {
			Map<?,?> m = mapOi.getMap(arguments[1].get());
			if(m == null) {
				return null;
			}
			Object outMap = mapOutputOi.create();
			for(Entry<?,?> e : m.entrySet()) {
				paramWrapper[0] = new DeferredJavaObject(e.getKey());
				paramWrapper[1] = new DeferredJavaObject(e.getValue());
				System.arraycopy(arguments, 2, paramWrapper, 2, arguments.length -2);
				Object booleanTest = udf.evaluate(bridge.deferr(paramWrapper));
				if(makroBooleanOi.get(booleanTest)) {
					mapOutputOi.put(outMap, e.getKey(), e.getValue());
				}
			}
			return outMap;
		}	
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("array_filter", children);
	}

}
