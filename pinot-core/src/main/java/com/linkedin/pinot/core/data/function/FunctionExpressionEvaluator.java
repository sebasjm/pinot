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

import java.util.List;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;

public class FunctionExpressionEvaluator {

  Node _rootNode;
  private String _columnName;

  public FunctionExpressionEvaluator(String columnName, TransformExpressionTree expressionTree) throws Exception {
    _columnName = columnName;
    _rootNode = planExecution(expressionTree);
  }

  public FunctionExpressionEvaluator(String columnName, String expression) throws Exception {
    TransformExpressionTree expressionTree = new Pql2Compiler().compileToExpressionTree(expression);
    _rootNode = planExecution(expressionTree);
  }

  private Node planExecution(TransformExpressionTree expressionTree) throws Exception {
    String transformName = expressionTree.getTransformName();
    List<TransformExpressionTree> children = expressionTree.getChildren();
    Class<?>[] argumentTypes = new Class<?>[children.size()];
    Node[] childNodes = new Node[children.size()];
    for (int i = 0; i < children.size(); i++) {
      TransformExpressionTree childExpression = children.get(i);
      Node childNode;
      switch (childExpression.getExpressionType()) {
      case FUNCTION:
        childNode = planExecution(childExpression);
        break;
      case IDENTIFIER:
        childNode = new ColumnExecutionNode(childExpression.toString());
        break;
      case LITERAL:
        childNode = new ConstantExecutionNode(childExpression.toString());
        break;
      default:
        throw new UnsupportedOperationException("Unsupported expression type:" + childExpression.getExpressionType());
      }
      childNodes[i] = childNode;
      argumentTypes[i] = childNode.getReturnType();
    }

    FunctionInfo functionInfo = FunctionRegistry.resolve(transformName, argumentTypes);
    return new FunctionExecutionNode(functionInfo, childNodes);
  }

  public Object evaluate(GenericRow row) {
    return _rootNode.execute(row);
  }

  private static interface Node {
    /**
     * 
     * @param row
     * @return
     */
    Object execute(GenericRow row);

    /**
     * 
     * @return
     */
    Class<?> getReturnType();

  }

  private static class FunctionExecutionNode implements Node {
    FunctionInvoker _functionInvoker;
    Node[] _argumentProviders;
    Object[] _argInputs;

    public FunctionExecutionNode(FunctionInfo functionInfo, Node[] argumentProviders) throws Exception {
      Preconditions.checkNotNull(functionInfo);
      Preconditions.checkNotNull(argumentProviders);
      _functionInvoker = new FunctionInvoker(functionInfo);
      _argumentProviders = argumentProviders;
      _argInputs = new Object[_argumentProviders.length];
    }

    public Object execute(GenericRow row) {
      for (int i = 0; i < _argumentProviders.length; i++) {
        _argInputs[i] = _argumentProviders[i].execute(row);
      }
      return _functionInvoker.process(_argInputs);
    }

    public Class<?> getReturnType() {
      return _functionInvoker.getReturnType();
    }
  }

  private static class ConstantExecutionNode implements Node {
    private String _constant;

    public ConstantExecutionNode(String constant) {
      this._constant = constant;
    }

    @Override
    public Object execute(GenericRow row) {
      return _constant;
    }

    @Override
    public Class<?> getReturnType() {
      return String.class;
    }

  }

  private static class ColumnExecutionNode implements Node {
    private String _column;

    public ColumnExecutionNode(String column) {
      this._column = column;
    }

    @Override
    public Object execute(GenericRow row) {
      return row.getValue(_column);
    }

    @Override
    public Class<?> getReturnType() {
      return Object.class;
    }

  }
}


