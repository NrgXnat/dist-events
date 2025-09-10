package io.xnatworks.events.distributed.components;

import org.springframework.jms.core.MessagePostProcessor;

import javax.jms.JMSException;
import javax.jms.Message;

public class DistEventsMessagePostProcessor implements MessagePostProcessor {
    public static final String MESSAGE_CLASS_PROPERTY = "messageClass";

    private final String messageClass;

    public DistEventsMessagePostProcessor(final Class<?> messageClass) {
        this.messageClass = messageClass.getSimpleName();
    }

    @Override
    public Message postProcessMessage(final Message message) throws JMSException {
        message.setStringProperty(MESSAGE_CLASS_PROPERTY, messageClass);
        return message;
    }
}
