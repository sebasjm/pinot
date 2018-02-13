/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.function;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.data.GenericRow;

public class FunctionExpressionEvaluatorTest {

  @Test
  public void testSimpleExpression() throws Exception {
    FunctionRegistry.registerStaticFunction(MyFunc.class.getDeclaredMethod("reverseString", String.class));
    String expression = "reverseString(testColumn)";

    FunctionExpressionEvaluator evaluator = new FunctionExpressionEvaluator("testColumn", expression);
    GenericRow row = new GenericRow();
    String value = "testColumnValue";
    row.putField("testColumn", value);
    Object result = evaluator.evaluate(row);
    Assert.assertEquals(result, new StringBuilder(value).reverse().toString());

  }

  static class MyFunc {

    public static String reverseString(String input) {
      return new StringBuilder(input).reverse().toString();
    }
  }
}
