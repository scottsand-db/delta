package io.delta.kernel.coordinatedcommits;

import io.delta.kernel.engine.Engine;
import java.util.Map;

public interface CommitCoordinatorBuilder {
  String getName();

  CommitCoordinatorClient build(Engine engine, Map<String, String> commitCoordinatorConf);
}
