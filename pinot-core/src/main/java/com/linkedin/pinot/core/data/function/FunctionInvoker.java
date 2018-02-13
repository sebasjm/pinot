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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class FunctionInvoker {
  Method _method;
  Object _instance;

  public FunctionInvoker(FunctionInfo info) throws Exception {
    _method = info.getMethod();
    Class<?> clazz = info.getClazz();
    if (Modifier.isStatic(_method.getModifiers())) {
      _instance = null;
    } else {
      _instance = clazz.newInstance();
    }
  }

  Class<?>[] getParameterTypes() {
    return _method.getParameterTypes();
  }

  public Class<?> getReturnType() {
    return _method.getReturnType();
  }

  Object process(Object[] args) {
    try {
      return _method.invoke(_instance, args);

    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      return null;
    }
  }

}
