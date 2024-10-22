/*
 * Copyright (2024) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.utils;

import io.delta.kernel.config.ConfigurationProvider;
import io.delta.kernel.coordinatedcommits.AbstractCommitCoordinatorBuilder;
import io.delta.kernel.engine.CommitCoordinatorClient;
import io.delta.kernel.internal.DeltaErrors;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class CommitCoordinatorUtils {
  private CommitCoordinatorUtils() {}

  public static CommitCoordinatorClient getCommitCoordinatorClient(
      String ccName, ConfigurationProvider sessionConfig, Map<String, String> ccConf) {
    final String builderConfKey = getCommitCoordinatorBuilderConfKey(ccName);

    if (!sessionConfig.contains(builderConfKey)) {
      throw DeltaErrors.unknownCommitCoordinator(ccName, builderConfKey);
    }

    final String builderClassName = sessionConfig.get(builderConfKey);

    try {
      return Class.forName(builderClassName)
          .asSubclass(AbstractCommitCoordinatorBuilder.class)
          .getConstructor()
          .newInstance()
          .build(sessionConfig, ccConf);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw DeltaErrors.couldNotInstantiateCommitCoordinatorClient(ccName, builderClassName, e);
    }
  }

  public static String getCommitCoordinatorBuilderConfKey(String ccName) {
    return String.format("io.delta.kernel.commitCoordinatorBuilder.%s.impl", ccName);
  }
}
