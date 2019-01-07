package com.trivago.dataegnineering.hive.udf.collectionhelper;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class TypeFromString extends GenericUDF {

	private ObjectInspector oi;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		String schema  = getConstantStringValue(arguments, 0);
		try {
			List<TypeInfo> infos = TypeInfoUtils.typeInfosFromTypeNames(Collections.singletonList(schema));
			oi = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(infos.get(0));
			return oi;
		}catch (Exception e) {
			throw new UDFArgumentException("couldn't get oi typeinfo from " + schema);
		}
		
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		return null;
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString(getFuncName(), children);
	}

}
