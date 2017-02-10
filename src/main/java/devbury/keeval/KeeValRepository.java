/*
 * Copyright 2017 David Noel
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

package devbury.keeval;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KeeValRepository<T> {

    private final KeeValManager manager;
    private final Class<T> clazz;

    public KeeValRepository(Class<T> clazz, KeeValManager manager) {
        this.manager = manager;
        this.clazz = clazz;
    }

    public Optional<T> findByKey(Object key) {
        return manager.findByKey(key, clazz);
    }

    public void update(Object key, T value) {
        manager.update(key, value);
    }

    public void createOrUpdate(Object key, T value) {
        manager.createOrUpdate(key, value);
    }

    public List<T> findAll() {
        return manager.findAll(clazz);
    }

    public Map<String, T> findAllAsMap() {
        return manager.findAllAsMap(clazz);
    }

    public void delete(Object key) {
        manager.delete(key, clazz);
    }

    public void create(Object key, T value) {
        manager.create(key, value);
    }
}
