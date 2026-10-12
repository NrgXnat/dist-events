package io.xnatworks.events.distributed.components.hibernate;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.xnatworks.events.distributed.components.AbstractEventMessage;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.event.spi.AbstractEvent;
import org.hibernate.event.spi.PostDeleteEvent;
import org.hibernate.event.spi.PostInsertEvent;
import org.hibernate.event.spi.PostUpdateEvent;

import java.io.Serial;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApiModel(description = "Supports propagating internal Hibernate modification events as distributed event messages.", parent = AbstractEventMessage.class)
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString
@Slf4j
public class HibernateEntityUpdateMessage extends AbstractEventMessage {
    @Serial
    private static final long serialVersionUID = 4666684937673104097L;

    public static final String       AE_TITLE            = "aeTitle";
    public static final String       ENABLED             = "enabled";
    public static final String       PORT                = "port";
    public static final List<String> CRITICAL_ATTRIBUTES = Stream.of(AE_TITLE, ENABLED, PORT).collect(Collectors.toList());

    @ApiModelProperty("Indicates the action that was taken on the entity.")
    private final Action action;

    @ApiModelProperty("Indicates the entity type (i.e. class name) of the affected entity.")
    private final String entityType;

    @ApiModelProperty("The ID of the affected entity.")
    private final Number id;

    @ApiModelProperty("Any extra properties required for downstream actions.")
    @Singular("property")
    private final Map<String, String> properties;

    /**
     * Builds the Hibernate event for this message.
     *
     * @param entityClass The resolved class for {@link #getEntityType()}
     * @param entity      The inserted or updated entity, as loaded on this node. Ignored for deletes, which carry a
     *                    placeholder because the entity is gone.
     *
     * @return The event for the message's action
     */
    public AbstractEvent toHibernateEvent(final Class<?> entityClass, final Object entity) {
        return switch (action) {
            case INSERT -> new PostInsertEvent(entity, id, null, null, null);
            case UPDATE -> new PostUpdateEvent(entity, id, null, null, null, null, null);
            // We can't retrieve the deleted entity, so put a placeholder entity in there.
            case DELETE -> new PostDeleteEvent(getPlaceholder(entityClass), id, null, null, null);
        };
    }

    private Object getPlaceholder(final Class<?> entityClass) {
        try {
            final Object placeholder = entityClass.getConstructor().newInstance();
            entityClass.getMethod("setId", id.getClass()).invoke(placeholder, id);
            return placeholder;
        } catch (NoSuchMethodException e) {
            log.warn("No setId() method found for entity class {}, just returning class name", entityClass);
            return entityClass.getName();
        } catch (Exception e) {
            throw new RuntimeException("Error creating placeholder instance for class " + entityType + " with ID " + id, e);
        }
    }
}
