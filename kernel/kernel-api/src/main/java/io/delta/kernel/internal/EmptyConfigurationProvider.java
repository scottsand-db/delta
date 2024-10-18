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

package io.delta.kernel.internal;

import io.delta.kernel.config.ConfigurationProvider;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * An implementation of {@link ConfigurationProvider} that always returns empty results or throws
 * exceptions for configuration values. This class is intended to be used in cases where no
 * configuration is provided or expected.
 */
public class EmptyConfigurationProvider implements ConfigurationProvider {
  @Override
  public String get(String key) {
    throw new NoSuchElementException();
  }

  @Override
  public Optional<String> getOptional(String key) {
    return Optional.empty();
  }

  @Override
  public boolean contains(String key) {
    return false;
  }
}
