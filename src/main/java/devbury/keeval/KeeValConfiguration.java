package devbury.keeval;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

@EnableConfigurationProperties(KeeValProperties.class)
public class KeeValConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(KeeValConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(ObjectMapperProvider.class)
    public ObjectMapperProvider objectMapperProvider() {
        return ObjectMapper::new;
    }

    @Bean
    @ConditionalOnMissingBean(KeeValManager.class)
    public KeeValManager keeValManager(DataSourceProvider dataSourceProvider,
                                       ObjectMapperProvider objectMapperProvider) {
        return new KeeValManager(dataSourceProvider, objectMapperProvider);
    }

    @Bean
    @ConditionalOnMissingBean(DataSourceProvider.class)
    public DataSourceProvider dataSourceProvider(KeeValProperties keeValProperties) {
        return () -> {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setUrl(String.format("jdbc:h2:%s", keeValProperties.getDbLocation()));
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            populator.addScript(new DefaultResourceLoader().getResource("classpath:/devbury/keeval/schema.sql"));
            populator.execute(dataSource);
            logger.info("datasource url {}", dataSource.getUrl());

            return dataSource;
        };
    }
}
