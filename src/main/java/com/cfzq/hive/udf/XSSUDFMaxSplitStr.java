/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringLength;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFLength.
 *
 */
@Description(name = "udf_max_split_str",
    value = "_FUNC_(str | binary) - Returns the length of str or number of bytes in binary data",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('Facebook') FROM src LIMIT 1;\n" + "  8")
@VectorizedExpressions({StringLength.class})
public class XSSUDFMaxSplitStr extends UDF {
	private final DoubleWritable result = new DoubleWritable();

  public DoubleWritable evaluate(Text s) throws HiveException {
	if (s == null || s.toString().trim().isEmpty()) return null;
	String[] arry = s.toString().split(",");
	if (arry == null || arry.length == 0) {
		result.set(0);
		return result;
	}
	Double max = -Double.MAX_VALUE;
	Double t = null;
	try {
		for (int i=0; i<arry.length; i++) {
			if (arry[i] == null || arry[i].trim().isEmpty()) {
				continue;
			}
			t = Double.parseDouble(arry[i]);
			if (t > max) {
				max = t;
			}
		}
	} catch (Exception e) {
		throw new HiveException("参数中包含非数字", e);
	}
	result.set(max);
	return result;
  }
  
  public static void main(String[] args) throws HiveException {
	  XSSUDFMaxSplitStr dd = new XSSUDFMaxSplitStr();
	  
	  System.out.println(dd.evaluate(new Text("-1,-3,-1,-5,-2")));
	  System.out.println(dd.evaluate(new Text("1,3,1,5,1")));
  }
}
