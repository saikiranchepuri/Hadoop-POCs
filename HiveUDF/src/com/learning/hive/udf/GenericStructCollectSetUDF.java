package com.learning.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCollectSet;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class GenericStructCollectSetUDF extends GenericUDAFCollectSet {

	// private StructObjectInspector soi;

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		if (parameters.length != 1) {
			throw new UDFArgumentTypeException(parameters.length - 1,
					"Exactly one argument is expected.");
		}
		if (parameters[0].getCategory() != ObjectInspector.Category.STRUCT) {
			throw new UDFArgumentTypeException(0,
					"Only struct type arguments are accepted but "
							+ parameters[0].getTypeName()
							+ " was passed as parameter 1.");
		}
		// soi = parameters[0];
		return new GenericUDAFStructEvaluator();

	}

}
