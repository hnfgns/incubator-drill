/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.drill.exec.vector.ValueVector;
import org.codehaus.janino.SimpleCompiler;

public class JaninoTest {
  private final static SimpleCompiler compiler = new SimpleCompiler();

  public static void main(String[] args) throws Exception {
    final String factoryCodePath = "exec/java-exec/src/main/java/org/apache/drill/exec/ValueVectorFactory.java";
    ValueVectorFactory factory = fromPath(factoryCodePath);
    ValueVector vector = factory.create();
    System.out.println(vector);
  }


  public static ValueVectorFactory fromPath(String path) throws Exception {
    final String code = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
    compiler.cook(new StringReader(code));
    Class klazz = compiler.getClassLoader().loadClass("org.apache.drill.exec.ValueVectorFactory$DefaultValueVectorFactory");
    return (ValueVectorFactory)klazz.newInstance();
  }
}
