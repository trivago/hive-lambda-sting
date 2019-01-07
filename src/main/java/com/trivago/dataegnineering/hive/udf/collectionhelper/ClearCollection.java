package com.trivago.dataegnineering.hive.udf.collectionhelper;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;

public class ClearCollection extends GenericUDF {

	
	
	private SettableMapObjectInspector mapOi;
	private SettableListObjectInspector listOi;
	
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments[0].getCategory() == ObjectInspector.Category.LIST
				&& arguments[0]  instanceof SettableListObjectInspector) {
			listOi = (SettableListObjectInspector) arguments[0];
			return listOi;
		} else if (arguments[0].getCategory() == ObjectInspector.Category.MAP
					&& arguments[0]  instanceof SettableMapObjectInspector) {
			mapOi = (SettableMapObjectInspector) arguments[0];
			return mapOi;
		}
		throw new UDFArgumentException(arguments[0].getClass().getName() + " is not a clearable collection type");
		
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		return arguments[0].get() == null ? 
					null : 
					listOi == null ? 
							mapOi.clear(arguments[0].get()) : 
							listOi.resize(arguments[0].get(),0);
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString(getFuncName(), children);
	}

}
