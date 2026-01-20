package io.xnatworks.events.distributed.components.hibernate;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.xnatworks.events.distributed.components.AbstractEventMessage;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ApiModel(description = "Supports propagating internal Hibernate modification events as distributed event messages.", parent = AbstractEventMessage.class)
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class HibernateEntityUpdateMessage extends AbstractEventMessage {
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
}
