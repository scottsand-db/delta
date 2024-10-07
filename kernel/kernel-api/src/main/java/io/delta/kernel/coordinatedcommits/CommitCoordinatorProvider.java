package io.delta.kernel.coordinatedcommits;

import io.delta.kernel.engine.Engine;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * CommitCoordinatorProvider is a singleton class responsible for managing the registration and
 * retrieval of {@link CommitCoordinatorBuilder} instances. It allows for the dynamic registration
 * of builders associated with commit-coordinator names, providing a way to map each name to a
 * specific builder implementation.
 *
 * <p>This class maintains a mapping from commit-coordinator names to their corresponding builders
 * and enforces that each name is registered with only one builder. Attempts to register the same
 * name with multiple builders will result in an exception.
 *
 * <p>Clients can retrieve {@link CommitCoordinatorClient} instances using the {@link
 * #getCommitCoordinatorClient(Engine, String, Map)} method, which will delegate the creation of the
 * client to the appropriate builder registered under the provided name.
 */
public class CommitCoordinatorProvider {

  //////////////////////////////
  // Static methods & members //
  //////////////////////////////

  private static CommitCoordinatorProvider INSTANCE;

  public static CommitCoordinatorProvider getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new CommitCoordinatorProvider();
    }
    return INSTANCE;
  }

  ////////////////////////////////
  // Instance methods & members //
  ////////////////////////////////

  /**
   * Mapping from different commit-coordinator names to the corresponding CommitCoordinatorBuilders.
   */
  private final Map<String, CommitCoordinatorBuilder> nameToBuilderMapping = new HashMap<>();

  private CommitCoordinatorProvider() {}

  /**
   * Registers a new {@link CommitCoordinatorBuilder} with this CommitCoordinatorProvider. Each
   * builder is associated with a unique commit-coordinator name.
   *
   * @param commitCoordinatorBuilder the {@link CommitCoordinatorBuilder} to register.
   * @throws IllegalArgumentException if a builder with the same name is already registered.
   */
  public synchronized void registerBuilder(CommitCoordinatorBuilder commitCoordinatorBuilder) {
    Optional<CommitCoordinatorBuilder> existingBuilder =
        Optional.ofNullable(nameToBuilderMapping.get(commitCoordinatorBuilder.getName()));

    if (existingBuilder.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "commit-coordinator %s is already registered with builder %s",
              existingBuilder.get().getName(), existingBuilder.get().getClass().getName()));
    } else {
      nameToBuilderMapping.put(commitCoordinatorBuilder.getName(), commitCoordinatorBuilder);
    }
  }

  /**
   * Retrieves a {@link CommitCoordinatorClient} instance by invoking the appropriate {@link
   * CommitCoordinatorBuilder} based on the commit-coordinator name.
   *
   * @param engine the {@link Engine} instance to use for building the client.
   * @param name the name of the commit-coordinator for which a client is being requested.
   * @param commitCoordinatorConf the configuration map to pass to the builder when constructing the
   *     client.
   * @return the {@link CommitCoordinatorClient} instance associated with the provided name.
   * @throws IllegalArgumentException if the commit-coordinator name is not registered.
   */
  public CommitCoordinatorClient getCommitCoordinatorClient(
      Engine engine, String name, Map<String, String> commitCoordinatorConf) {
    CommitCoordinatorBuilder builder = nameToBuilderMapping.get(name);
    if (builder == null) {
      throw new IllegalArgumentException(String.format("Unknown commit-coordinator: %s", name));
    }
    return builder.build(engine, commitCoordinatorConf);
  }
}
