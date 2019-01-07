package com.trivago.dataegnineering.hive.udf.collectionhelper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import com.trivago.dataegnineering.hive.udf.collectionhelper.utils.GenericUDFParamBridge;
import com.trivago.dataegnineering.hive.udf.collectionhelper.utils.copyconverter.TargetDrivenCopyCreatingConverter;

public class MapCollection extends GenericUDF implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final String KEY = "key";
	private static final String VALUE = "value";
	
	private GenericUDF udf;
	transient private DeferredObject[] paramWrapper;
	transient private ObjectInspector makroReturnOi;
	transient private StructObjectInspector makroStruct;
	transient private MapObjectInspector mapOi;
	transient private ListObjectInspector listOi;
	transient private StandardListObjectInspector listOutputOi;
	transient private StandardMapObjectInspector mapOutputOi;
	transient private GenericUDFParamBridge bridge;
	transient private Converter copyMakroOutput;
	
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		String functionName = getConstantStringValue(arguments, 0);
		if(udf == null) {
			FunctionInfo fi;
			try {
				fi = FunctionRegistry.getFunctionInfo(functionName);
				udf = fi.getGenericUDF();
				udf.getClass();
			} catch (SemanticException e) {
				throw new UDFArgumentException(e);
			}	
			
		}
		
		if(arguments[1].getCategory() == ObjectInspector.Category.LIST) {
			listOi = (ListObjectInspector) arguments[1];
			ObjectInspector[] allArgs = new ObjectInspector[1 + arguments.length - 2];
			paramWrapper = new DeferredObject[allArgs.length];
			allArgs[0] = listOi.getListElementObjectInspector();
			System.arraycopy(arguments, 2, allArgs, 1, arguments.length - 2);
			bridge = GenericUDFParamBridge.getParameterInspectors(udf, allArgs);
			makroReturnOi = udf.initialize(bridge.getOis());
		} else if(arguments[1].getCategory() == ObjectInspector.Category.MAP) {
			mapOi = (MapObjectInspector) arguments[1];
			ObjectInspector[] allArgs = new ObjectInspector[2 + arguments.length - 2];
			paramWrapper = new DeferredObject[allArgs.length];
			allArgs[0] = mapOi.getMapKeyObjectInspector();
			allArgs[1] = mapOi.getMapValueObjectInspector();
			System.arraycopy(arguments, 2, allArgs, 2, arguments.length - 2);
			bridge = GenericUDFParamBridge.getParameterInspectors(udf, allArgs);
			makroReturnOi = udf.initialize(bridge.getOis());
		}else {
			throw new UDFArgumentException("second param needs to be a list or map");
		}
		copyMakroOutput = TargetDrivenCopyCreatingConverter.getConverter(makroReturnOi, makroReturnOi);
		
		if(makroReturnOi.getCategory() == ObjectInspector.Category.STRUCT) {
			makroStruct = (StructObjectInspector) makroReturnOi;
			try {
				StructField keyField = null;
				StructField valField = null;
				
				if((keyField = makroStruct.getStructFieldRef(KEY)) != null
				  && (valField = makroStruct.getStructFieldRef(VALUE)) != null){
					mapOutputOi = ObjectInspectorFactory.getStandardMapObjectInspector(keyField.getFieldObjectInspector(), valField.getFieldObjectInspector());
					return mapOutputOi;
				}
			} catch (Exception e) {
				//falling back to default list as return type case
			}
		} 
		
		listOutputOi =  ObjectInspectorFactory.getStandardListObjectInspector(makroReturnOi);
	
		return listOutputOi;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if(listOi != null) {
			List<?> l =  listOi.getList(arguments[1].get());
			if(l == null) {
				return null;
			}
			if(listOutputOi != null) {
				Object OiList =  listOutputOi.create(l.size());
				for(int i = 0;i < l.size() ; i++) {
					paramWrapper[0] = new DeferredJavaObject( l.get(i));
					System.arraycopy(arguments, 2, paramWrapper, 1, arguments.length -2);
					listOutputOi.set(OiList, i,copyMakroOutput.convert(udf.evaluate(bridge.deferr(paramWrapper))));
				}
				return OiList;
			} else  {
				Object outMap = mapOutputOi.create();
				StructField keyField = makroStruct.getStructFieldRef(KEY);
				StructField valField = makroStruct.getStructFieldRef(VALUE);
				for(int i = 0;i < l.size() ; i++) {
					paramWrapper[0] = new DeferredJavaObject(l.get(i));
					System.arraycopy(arguments, 2, paramWrapper, 1, arguments.length -2);
					Object converted = copyMakroOutput.convert(udf.evaluate(bridge.deferr(paramWrapper)));
					mapOutputOi.put(outMap, makroStruct.getStructFieldData(converted, keyField), makroStruct.getStructFieldData(converted, valField));
				}
				return outMap;
			}
			
		}else {
			Map<?,?> m = mapOi.getMap(arguments[1].get());
			if(m == null) {
				return null;
			}
			if(listOutputOi != null) {
				Object OiList =  listOutputOi.create(m.size());
				int i = 0;
				for(Entry<?,?> e : m.entrySet()) {
					paramWrapper[0] = new DeferredJavaObject(e.getKey());
					paramWrapper[1] = new DeferredJavaObject(e.getValue());
					System.arraycopy(arguments, 2, paramWrapper, 2, arguments.length -2);
					listOutputOi.set(OiList, i,copyMakroOutput.convert(udf.evaluate(bridge.deferr(paramWrapper))));
					i++;
				}
				return OiList;
			} else {
				Object outMap = mapOutputOi.create();
				StructField keyField = makroStruct.getStructFieldRef(KEY);
				StructField valField = makroStruct.getStructFieldRef(VALUE);
				for(Entry<?,?> e : m.entrySet()) {
					paramWrapper[0] = new DeferredJavaObject(e.getKey());
					paramWrapper[1] = new DeferredJavaObject(e.getValue());
					System.arraycopy(arguments, 2, paramWrapper, 2, arguments.length -2);
					Object converted = copyMakroOutput.convert(udf.evaluate(bridge.deferr(paramWrapper)));
					mapOutputOi.put(outMap, makroStruct.getStructFieldData(converted, keyField), makroStruct.getStructFieldData(converted, valField));
				}
				return outMap;
			}
		}
		
	}
	
	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("array_map", children);
	}


}
