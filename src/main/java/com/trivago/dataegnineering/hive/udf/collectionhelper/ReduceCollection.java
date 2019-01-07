package com.trivago.dataegnineering.hive.udf.collectionhelper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;

import com.trivago.dataegnineering.hive.udf.collectionhelper.utils.GenericUDFParamBridge;
import com.trivago.dataegnineering.hive.udf.collectionhelper.utils.copyconverter.TargetDrivenCopyCreatingConverter;


public class ReduceCollection extends GenericUDF implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private GenericUDF udf;
	private boolean typeHintasInitialVal = true; 

	transient private MapObjectInspector mapOi;	
	transient private ListObjectInspector listOi;
	
	transient private Converter makroToMakroParameter;
	
	
	transient private DeferredJavaObject[] paramWrapper;
	transient private ObjectInspector makroReturnOi;
	
	transient private int skipOverBooleanOptionalNum;
	
	transient private GenericUDFParamBridge bridge;
	
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if(arguments.length < 3) {
			throw new UDFArgumentException("need a string and an array and a reference to the output type");
		}
		
		String functionName = getConstantStringValue(arguments, 0);
		if(udf == null) {
			FunctionInfo fi;
			try {
					fi = FunctionRegistry.getFunctionInfo(functionName);
					udf = fi.getGenericUDF();
			} catch (Exception e) {
					throw new UDFArgumentException("Failed to load UDF" + functionName + " from "+getFuncName() +
							"caused by " + e.getClass().getName() + ":" + e.getMessage());
			}	
		}
		
		
		if(arguments.length > 3) {
			if (arguments[3].getCategory() == ObjectInspector.Category.PRIMITIVE 
				&& ((PrimitiveObjectInspector) arguments[3]).getPrimitiveCategory() ==
					PrimitiveCategory.BOOLEAN 
				&& arguments[3] instanceof ConstantObjectInspector) {
				skipOverBooleanOptionalNum = 1;
				typeHintasInitialVal = ((BooleanObjectInspector) arguments[3]).get(((ConstantObjectInspector) arguments[3] ).getWritableConstantValue());
			} else {
				throw new UDFArgumentException("fourth param would need to be a const boolean");
			}
		}
		
		if(arguments[1].getCategory() == ObjectInspector.Category.LIST) {
			listOi = (ListObjectInspector) arguments[1];
			ObjectInspector[] allParams = new ObjectInspector[2 + arguments.length -3 -skipOverBooleanOptionalNum];
			paramWrapper = new DeferredJavaObject[allParams.length];
			allParams[0] = listOi.getListElementObjectInspector();
			allParams[1] = arguments[2]; // iteration variable, then skip the boolean
			System.arraycopy(arguments, 3 + skipOverBooleanOptionalNum, allParams, 2, arguments.length -3 -skipOverBooleanOptionalNum);
			bridge = GenericUDFParamBridge.getParameterInspectors(udf, allParams);
			makroReturnOi = udf.initialize(bridge.getOis());
			try {
				makroToMakroParameter = TargetDrivenCopyCreatingConverter.getConverter(makroReturnOi,bridge.getOis()[1]);
			} catch (Exception e) {
				throw new UDFArgumentException(e);
			}
			
		} else if(arguments[1].getCategory() == ObjectInspector.Category.MAP)
		{
			mapOi = (MapObjectInspector) arguments[1];
			ObjectInspector[] allParams = new ObjectInspector[2 + arguments.length -2 -skipOverBooleanOptionalNum];
			paramWrapper = new DeferredJavaObject[allParams.length];
			allParams[0] = mapOi.getMapKeyObjectInspector();
			allParams[1] = mapOi.getMapValueObjectInspector();
			allParams[2] = arguments[2];
			System.arraycopy(arguments, 2 +skipOverBooleanOptionalNum, allParams, 3, arguments.length -3 -skipOverBooleanOptionalNum); 
			bridge = GenericUDFParamBridge.getParameterInspectors(udf, allParams);
			makroReturnOi = udf.initialize(bridge.getOis());
			try {
				makroToMakroParameter = TargetDrivenCopyCreatingConverter.getConverter(makroReturnOi,bridge.getOis()[2]);
			} catch (Exception e) {
				throw new UDFArgumentException(e);
			}
		}else {
			throw new UDFArgumentException("second param needs to be a list or map");
		}
		if(!ObjectInspectorUtils.compareTypes(arguments[2], makroReturnOi)) {
			throw new UDFArgumentException("Makro needs to retun same type as the type of the second parameter, but returns:"+
											makroReturnOi.getTypeName() + "the parameter indicated is:" + arguments[2].getTypeName());
		}
		

		return makroReturnOi;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if(listOi != null) {
			List<?> l =  listOi.getList(arguments[1].get());
			if(l == null) {
				return null;
			}
			Object makroData =  typeHintasInitialVal ? arguments[2].get() :  null;
			for(int i = 0;i < l.size() ; i++) {
				paramWrapper[0] = new DeferredJavaObject(l.get(i));
				paramWrapper[1] = new DeferredJavaObject(i == 0 ? makroData : makroToMakroParameter.convert(makroData));
				System.arraycopy(arguments, 3 +skipOverBooleanOptionalNum, paramWrapper, 2, arguments.length -3 -skipOverBooleanOptionalNum);
				makroData = udf.evaluate(bridge.deferr(paramWrapper));
			}
			return makroData;
		} else {
			Map<?,?> m =  mapOi.getMap(arguments[1].get());
			if(m == null) {
				return null;
			}
			Object makroData =  typeHintasInitialVal ? arguments[2].get() :  null;
			boolean isFirst = true;
			for(Entry<?,?> e : m.entrySet()) {
				paramWrapper[0] = new DeferredJavaObject(e.getKey());
				paramWrapper[1] = new DeferredJavaObject(e.getValue());
				paramWrapper[2] = new DeferredJavaObject(isFirst ? makroData : makroToMakroParameter.convert(makroData));
				System.arraycopy(arguments, 2 +skipOverBooleanOptionalNum, paramWrapper, 3, arguments.length -3 -skipOverBooleanOptionalNum); 
				makroData = udf.evaluate(bridge.deferr(paramWrapper));
				isFirst = false;
			}
			return makroData;	
		}
		
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("array_reduce", children);
	}


}
