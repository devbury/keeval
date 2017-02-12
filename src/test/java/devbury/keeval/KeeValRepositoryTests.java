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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {KeeValConfiguration.class, KeeValRepositoryTests.class})
public class KeeValRepositoryTests {

    @Autowired
    KeeValManager keeValManager;

    @Bean
    public DataSourceProvider dataSourceProvider() {
        return () -> new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:/devbury/keeval/schema.sql")
                .build();
    }

    @Test
    public void stringRepositoryTest() {
        KeeValRepository<String> repo = keeValManager.repository(String.class);

        assertThat(repo.findByKey("key1").isPresent(), is(false));
        repo.create("key1", "value1");
        repo.create("key2", "value2");

        assertThat(repo.findByKey("key1").get(), is(equalTo("value1")));
        assertThat(repo.findByKey("key2").get(), is(equalTo("value2")));

        List<String> values = repo.findAll();
        assertThat(values.size(), is(equalTo(2)));
        assertThat(values, containsInAnyOrder("value1", "value2"));

        Map<String, String> map = repo.findAllAsMap();
        assertThat(map.size(), is(equalTo(2)));
        assertThat(map.entrySet(), containsInAnyOrder(
                new AbstractMap.SimpleEntry<>("key1", "value1"),
                new AbstractMap.SimpleEntry<>("key2", "value2"))
        );

        repo.update("key3", "value3");
        assertThat(repo.findByKey("key3").isPresent(), is(false));

        repo.createOrUpdate("key3", "value3");
        assertThat(repo.findByKey("key3").get(), is("value3"));

        repo.createOrUpdate("key3", "value33");
        assertThat(repo.findByKey("key3").get(), is("value33"));

        assertThat(repo.findAll().size(), is(equalTo(3)));

        repo.delete("key3");
        assertThat(repo.findByKey("key3").isPresent(), is(false));

        assertThat(repo.findAll().size(), is(equalTo(2)));

        assertThat(keeValManager.findRawByKey("key3", String.class).isPresent(), is(false));

        assertThat(keeValManager.findRawByKey("key1", String.class).get(), is(equalTo("\"value1\"")));

        assertThat(keeValManager.findAllRawAsMap(String.class).get("key1"), is(equalTo("\"value1\"")));

    }
}
