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
      String ccName,
      ConfigurationProvider sessionConfig,
      Map<String, String> ccConf) {
    final String builderConfKey =
        String.format("io.delta.kernel.commitCoordinatorBuilder.%s.impl", ccName);

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
}
