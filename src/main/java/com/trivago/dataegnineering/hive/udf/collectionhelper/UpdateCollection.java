package com.trivago.dataegnineering.hive.udf.collectionhelper;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;

import com.trivago.dataegnineering.hive.udf.collectionhelper.utils.copyconverter.TargetDrivenCopyCreatingConverter;

public class UpdateCollection extends GenericUDF {
	
	transient private SettableListObjectInspector listOi;
	transient private SettableMapObjectInspector mapOi;
	transient private Converter mapKeyConverter;
	transient private Converter valueConverter;
	
	

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		
		if(arguments[0].getCategory() == Category.LIST &&
				arguments[0] instanceof SettableListObjectInspector /* not converting to own atm*/ &&
				arguments.length == 2) {
			listOi = (SettableListObjectInspector) arguments[0];
			if(!ObjectInspectorUtils.compareTypes(arguments[1], listOi.getListElementObjectInspector())){
				throw new UDFArgumentException("list type and argument doesnt match " + listOi.getTypeName() + " and " +arguments[1]);
			}
			valueConverter = ObjectInspectorConverters.getConverter(arguments[1], listOi.getListElementObjectInspector());
			
			
		} else if (arguments[0].getCategory() == Category.MAP &&
				arguments[0] instanceof SettableMapObjectInspector /* not converting to own atm*/ &&
				arguments.length == 3)
		{
			mapOi = (SettableMapObjectInspector) arguments[0];
			if(!ObjectInspectorUtils.compareTypes(arguments[1], mapOi.getMapKeyObjectInspector()) ||
			!ObjectInspectorUtils.compareTypes(arguments[2], mapOi.getMapValueObjectInspector())){
				throw new UDFArgumentException("Map key and value types need to match "
						+  "map: " + mapOi.getTypeName() + " key: " + arguments[1].getTypeName() 
						+ " value: " +arguments[2].getTypeName());
			}

			mapKeyConverter  = TargetDrivenCopyCreatingConverter.getConverter(arguments[1], mapOi.getMapKeyObjectInspector());
			valueConverter  = TargetDrivenCopyCreatingConverter.getConverter(arguments[2], mapOi.getMapValueObjectInspector());
			
			
		} else {
			throw new UDFArgumentException("first params needs to be a map or list not " 
											+ arguments[0].getTypeName() + "/" + arguments[0].getClass().getName()
											+ "a map needs also 2 extra params, a list extra 1 total is:" + arguments.length);
		}
		
		
		return arguments[0];
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if(mapOi != null) {
			Object theMap = arguments[0].get();
			theMap = theMap == null ? mapOi.create() : theMap;
			theMap = mapOi.put(theMap, mapKeyConverter.convert(arguments[1].get()), valueConverter.convert(arguments[2].get()));
			return theMap;
		} else {
			Object theList = arguments[0].get();
			theList = theList == null ? listOi.create(0) : theList;
			theList = listOi.resize(theList, listOi.getListLength(theList)+1);
			theList = listOi.set(theList, listOi.getListLength(theList) -1 , valueConverter.convert(arguments[1].get()));
			return theList;
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString(getFuncName(), children);
	}

}
