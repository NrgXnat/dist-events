package io.xnatworks.events.distributed.components;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import javax.annotation.Nonnull;

@SuppressWarnings("unused")
@Slf4j
public class IsMultiNodeDeployment implements Condition {
    private static final String QUERY_IS_MULTI_NODE = "SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT node_id FROM xhbm_xnat_node_info) AS temp) > 1";

    @Override
    public boolean matches(final ConditionContext context, final @Nonnull AnnotatedTypeMetadata metadata) {
        final ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
        if (beanFactory == null) {
            log.warn("Unable to get bean factory from context, cannot determine if this is a multi-node deployment.");
            return false;
        }
        // I thought about filtering and checking only the count of *active* nodes, but any inactive
        // nodes could be activated, so just prepare for that.
        // TODO: This happens so early that nothing is really available: no JdbcTemplate, no DataSource, etc.
        return true; // Boolean.TRUE.equals(beanFactory.getBean(JdbcTemplate.class).queryForObject(QUERY_IS_MULTI_NODE, Boolean.class));
    }
}
