package io.delta.kernel.defaults.internal.coordinatedcommits;

import io.delta.kernel.coordinatedcommits.CommitCoordinatorBuilder;
import io.delta.kernel.coordinatedcommits.CommitCoordinatorClient;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class CommitCoordinatorProvider {

  private CommitCoordinatorProvider() {}

  /////////////////
  // Public APIs //
  /////////////////

  public static CommitCoordinatorClient getCommitCoordinatorClient(
      Configuration hadoopConf,
      String commitCoordinatorName,
      Map<String, String> commitCoordinatorConf) {
    final String builderConfKey = getCommitCoordinatorBuilderConfKey(commitCoordinatorName);

    final String builderClassName = hadoopConf.get(builderConfKey);

    if (builderClassName == null) {
      throw new IllegalArgumentException(
          String.format("Unknown commit coordinator: %s", commitCoordinatorName)
      );
    }

    final Map<String, String> systemConf = new HashMap<>();
    for (Map.Entry<String, String> entry : hadoopConf) {
      systemConf.put(entry.getKey(), entry.getValue());
    }

    final CommitCoordinatorBuilder builder = instantiateCommitCoordinatorBuilder(builderClassName);

    return builder.build(systemConf, commitCoordinatorConf);
  }

  ////////////////////////////
  // Private helper methods //
  ////////////////////////////

  private static String getCommitCoordinatorBuilderConfKey(String commitCoordinatorName) {
    return "io.delta.kernel.commitCoordinatorBuilder." + commitCoordinatorName + ".impl";
  }

  private static CommitCoordinatorBuilder instantiateCommitCoordinatorBuilder(String className) {
    try {
      return Class
          .forName(className)
          .asSubclass(CommitCoordinatorBuilder.class)
          .getConstructor()
          .newInstance();
    } catch (Exception ex) {
      throw new IllegalArgumentException(
          String.format("Failed to instantiate Commit Coordinator Builder: %s", className),
          ex
      );
    }
  }
}
