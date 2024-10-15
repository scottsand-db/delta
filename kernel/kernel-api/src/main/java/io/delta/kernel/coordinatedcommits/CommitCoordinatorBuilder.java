package io.delta.kernel.coordinatedcommits;

import java.util.Map;

public abstract class CommitCoordinatorBuilder {
  public CommitCoordinatorBuilder() { }

  public abstract String getName();

  public abstract CommitCoordinatorClient build(
      Map<String, String> systemConf,
      Map<String, String> commitCoordinatorConf);
}
