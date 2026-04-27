package io.xnatworks.events.distributed.components.xft.methods;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.nrg.framework.exceptions.NotFoundException;
import org.nrg.framework.exceptions.NrgServiceError;
import org.nrg.framework.exceptions.NrgServiceRuntimeException;
import org.nrg.xdat.display.DisplayManager;
import org.nrg.xdat.display.ElementDisplay;
import org.nrg.xdat.display.transport.entities.ElementDisplayDB;
import org.nrg.xdat.display.transport.services.ElementDisplayStorageService;
import org.nrg.xdat.security.ElementSecurity;
import org.nrg.xft.event.XftItemEventI;
import org.nrg.xft.event.methods.AbstractXftItemEventHandlerMethod;
import org.nrg.xft.schema.XFTElement;
import org.nrg.xft.schema.XFTManager;
import org.nrg.xft.schema.XFTSchema;
import org.nrg.xft.schema.db.entities.DBBackedSchema;
import org.nrg.xft.schema.db.services.DBBackedSchemaService;
import org.nrg.xnat.services.LoadDBDataTypeCallable;
import org.nrg.xnat.services.LoadDBDataTypeResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.nrg.xnat.services.CreateDataTypeCallable.SCHEMA_ID;
import static io.xnatworks.events.distributed.components.xft.MultiNodeXftUpdateHandlerMethod.CRITERIA_DATA_TYPE_CREATED;

@Component
@Slf4j
public class XdatElementSecurityUpdateHandlerMethod extends AbstractXftItemEventHandlerMethod {
    private static final String SUCCESS = "successfully";
    private static final String FAILURE = "unsuccessfully";

    private final DBBackedSchemaService        dbBackedSchemaService;
    private final ElementDisplayStorageService elementDisplayStorageService;

    public XdatElementSecurityUpdateHandlerMethod(final DBBackedSchemaService dbBackedSchemaService, final ElementDisplayStorageService elementDisplayStorageService) {
        super(CRITERIA_DATA_TYPE_CREATED);
        this.dbBackedSchemaService        = dbBackedSchemaService;
        this.elementDisplayStorageService = elementDisplayStorageService;
    }

    @Override
    protected boolean handleEventImpl(final XftItemEventI event) {
        final String         action     = event.getAction();
        final String         xsiType    = event.getXsiType();
        final String         dataType   = event.getId();
        final Map<String, ?> properties = event.getProperties();
        final Long           schemaId   = (Long) properties.get(SCHEMA_ID);

        if (!StringUtils.equals(action, XftItemEventI.CREATE) || !StringUtils.equals(xsiType, ElementSecurity.SCHEMA_ELEMENT_NAME)) {
            log.warn("This handler method is for handling newly created data types and must have the action {} and XSI type {}", XftItemEventI.CREATE, ElementSecurity.SCHEMA_ELEMENT_NAME);
            return false;
        }
        if (StringUtils.isBlank(dataType)) {
            log.warn("This handler method is for handling newly created data types and must find the new XSI type as the ID, but here it's blank.");
            return false;
        }
        if (!properties.containsKey(SCHEMA_ID)) {
            log.warn("This handler method is for handling newly created data types and must find the ID for the database-backed schema entity for the data type {} in the properties map, but it's not provided here.", dataType);
            return false;
        }
        if (schemaId == null || schemaId == 0) {
            log.warn("This handler method is for handling newly created data types and must find the ID for the database-backed schema entity for the data type {} in the properties map: it was provided but is: {}", dataType, schemaId);
            return false;
        }

        log.info("Handling creation of new data type '{}' based on schema entity {}", dataType, schemaId);
        try {
            final LoadDBDataTypeResult result  = new LoadDBDataTypeCallable(schemaId, dbBackedSchemaService, elementDisplayStorageService).call();
            final boolean              success = result.isSuccess();
            doubleCheckDisplays(schemaId);
            log.info("Handled a new data type {} from schema ID {} and {} loaded the data-type schema for it", dataType, schemaId, success ? SUCCESS : FAILURE);
            return success;
        } catch (Exception e) {
            throw new NrgServiceRuntimeException(NrgServiceError.Unknown, "An unknown error occurred trying to load the data type " + xsiType + " from schema " + schemaId, e);
        }
    }

    // Bypasses DBBackedSchemaService.getElementNames() because its static cache can be poisoned with
    // an empty list before this schema is registered in XFTManager. We walk XFTManager directly instead.
    private void doubleCheckDisplays(final Long schemaId) throws NotFoundException {
        final DBBackedSchema dbschema     = dbBackedSchemaService.get(schemaId);
        final List<String>   elementNames = collectElementNamesFromXftManager(dbschema);
        log.debug("Double-checking display docs for schema {} elements: {}", dbschema.getName(), elementNames);

        for (final String elementName : elementNames) {
            if (DisplayManager.GetElementDisplay(elementName) != null) {
                log.debug("ElementDisplay already loaded for: {}", elementName);
                continue;
            }
            final ElementDisplayDB storedDisplay = elementDisplayStorageService.findByElementName(elementName);
            if (storedDisplay == null) {
                log.warn("No stored ElementDisplay found for: {}", elementName);
                continue;
            }
            final ElementDisplay ed = elementDisplayStorageService.renderElementDisplay(storedDisplay);
            DisplayManager.GetInstance().addElement(ed);
            log.debug("ElementDisplay loaded from storage for: {}", elementName);
        }
    }

    private List<String> collectElementNamesFromXftManager(final DBBackedSchema dbschema) {
        final List<String> names = new ArrayList<>();
        for (final XFTSchema schema : XFTManager.GetSchemas()) {
            if (StringUtils.equals(dbschema.getName(), schema.getDataModel().getFileName())) {
                for (final Object o : schema.getSortedElements()) {
                    final XFTElement element = (XFTElement) o;
                    names.add(element.getType().getFullForeignType());
                }
            }
        }
        return names;
    }
}
