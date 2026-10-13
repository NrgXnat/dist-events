package io.xnatworks.events.distributed.components.hibernate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.event.spi.AbstractEvent;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nrg.framework.services.NrgEventServiceI;
import org.nrg.xnat.hibernate.listeners.methods.HibernateEntityEventHandlerMethod;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.hibernate5.LocalSessionFactoryBuilder;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Runs the listener against PostgreSQL with XNAT's Hibernate setup: a Spring-built session factory, HikariCP and a
 * JCache second-level cache. Each test covers one way the listener failed when it loaded entities through a single
 * long-lived session with no transaction. Skipped where Docker isn't available.
 */
@Testcontainers(disabledWithoutDocker = true)
class TestHibernateEntityUpdateTopicListener {
    private static final String THIS_NODE  = "node-b";
    private static final String OTHER_NODE = "node-a";

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15-alpine");

    private static HikariDataSource            dataSource;
    private static SessionFactory              sessionFactory;
    private static HibernateTransactionManager transactionManager;
    private static TransactionTemplate         transaction;

    @BeforeAll
    static void setUpDatabase() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        // One connection, so recoversWhenTheDatabaseClosesItsConnections knows which connection the pool hands out
        config.setMaximumPoolSize(1);
        dataSource = new HikariDataSource(config);

        final LocalSessionFactoryBuilder builder = new LocalSessionFactoryBuilder(dataSource);
        builder.addAnnotatedClasses(LargeObjectEntity.class);
        builder.setProperty(AvailableSettings.DIALECT, PostgreSQL10Dialect.class.getName());
        builder.setProperty(AvailableSettings.HBM2DDL_AUTO, "create-drop");
        builder.setProperty(AvailableSettings.USE_SECOND_LEVEL_CACHE, "true");
        builder.setProperty(AvailableSettings.CACHE_REGION_FACTORY, "jcache");
        builder.setProperty("hibernate.javax.cache.provider", "org.ehcache.jsr107.EhcacheCachingProvider");
        builder.setProperty("hibernate.javax.cache.missing_cache_strategy", "create");
        builder.setProperty(AvailableSettings.GENERATE_STATISTICS, "true");
        sessionFactory     = builder.buildSessionFactory();
        transactionManager = new HibernateTransactionManager(sessionFactory);
        transaction        = new TransactionTemplate(transactionManager);
    }

    @AfterAll
    static void tearDownDatabase() {
        sessionFactory.close();
        dataSource.close();
    }

    @BeforeEach
    void clearCacheAndStatistics() {
        sessionFactory.getCache().evictAllRegions();
        getStatistics().clear();
    }

    /**
     * The shared session kept every entity it loaded, so a node's heap filled with entities changed on other nodes.
     */
    @Test
    void releasesEachLoadedEntityWithItsSession() {
        final List<Integer>                      ids      = createEntities(50);
        final RecordingHandlerMethod             handler  = updateHandler();
        final HibernateEntityUpdateTopicListener listener = listener(handler);
        getStatistics().clear();

        ids.forEach(id -> listener.receive(updateFromOtherNode(id)));

        assertEquals(50, handler.getEntities().size());
        assertEquals(50, getStatistics().getSessionOpenCount());
        assertEquals(50, getStatistics().getSessionCloseCount());
    }

    /**
     * The shared session read in auto-commit mode, where PostgreSQL's driver refuses large objects.
     */
    @Test
    void loadsEntitiesWithLargeObjectColumns() {
        final Integer                id      = createEntities(1).getFirst();
        final RecordingHandlerMethod handler = updateHandler();
        getStatistics().clear();

        listener(handler).receive(updateFromOtherNode(id));

        assertEquals(1, getStatistics().getEntityLoadCount());
        assertEquals(1, handler.getEntities().size());
        assertEquals("payload-0", handler.getLastEntity().getPayload());
    }

    /**
     * The shared session held one connection between messages, so once the database closed it every later message
     * failed until a restart.
     */
    @Test
    void recoversWhenTheDatabaseClosesItsConnections() throws SQLException {
        final List<Integer>                      ids      = createEntities(3);
        final RecordingHandlerMethod             handler  = updateHandler();
        final HibernateEntityUpdateTopicListener listener = listener(handler);

        listener.receive(updateFromOtherNode(ids.get(0)));
        terminateOtherConnections();
        // Whether this one fails on the closed connection depends on whether the pool checks it before handing it
        // out. Either way the pool replaces it, so the next message loads.
        listener.receive(updateFromOtherNode(ids.get(1)));
        listener.receive(updateFromOtherNode(ids.get(2)));

        assertEquals(ids.get(2), handler.getLastEntity().getId());
    }

    @Test
    void leavesNoCachedCopyOfTheEntityItLoads() {
        final Integer id = createEntities(1).getFirst();

        listener(updateHandler()).receive(updateFromOtherNode(id));

        assertFalse(isCached(id));
    }

    @Test
    void skipsHandlersWhenTheEntityIsGone() {
        final RecordingHandlerMethod handler = updateHandler();

        listener(handler).receive(updateFromOtherNode(Integer.MAX_VALUE));

        assertTrue(handler.getEntities().isEmpty());
    }

    @Test
    void evictsTheCachedEntityWithoutLoadingItWhenNoHandlerWantsIt() {
        final Integer id = createEntities(1).getFirst();
        cache(id);
        getStatistics().clear();

        listener().receive(updateFromOtherNode(id));

        assertFalse(isCached(id));
        assertEquals(0, getStatistics().getSessionOpenCount());
    }

    /**
     * The listener used to load the entity before evicting it, so any failure to load left a stale cached copy.
     */
    @Test
    void evictsTheCachedEntityEvenWhenLoadingItFails() {
        final Integer id = createEntities(1).getFirst();
        cache(id);
        final RecordingHandlerMethod     handler     = updateHandler();
        final PlatformTransactionManager unavailable = mock(PlatformTransactionManager.class);
        when(unavailable.getTransaction(any())).thenThrow(new CannotCreateTransactionException("Database unavailable"));

        listener(unavailable, handler).receive(updateFromOtherNode(id));

        assertFalse(isCached(id));
        assertTrue(handler.getEntities().isEmpty());
    }

    @Test
    void handlesDeletesWithoutTheDatabase() {
        final Integer                id      = createEntities(1).getFirst();
        final RecordingHandlerMethod handler = new RecordingHandlerMethod(HibernateEntityEventHandlerMethod.Action.DELETE);
        getStatistics().clear();

        listener(handler).receive(fromOtherNode(Action.DELETE, id));

        // LargeObjectEntity has no setId(), so the placeholder is the class name
        assertEquals(List.of(LargeObjectEntity.class.getName()), handler.getEntities());
        assertEquals(0, getStatistics().getSessionOpenCount());
    }

    @Test
    void survivesAHandlerFailure() {
        final Integer                id      = createEntities(1).getFirst();
        final RecordingHandlerMethod failing = new RecordingHandlerMethod(HibernateEntityEventHandlerMethod.Action.UPDATE) {
            @Override
            public boolean matches(final AbstractEvent event) {
                throw new IllegalStateException("Handler failure");
            }
        };

        assertDoesNotThrow(() -> listener(failing).receive(updateFromOtherNode(id)));
    }

    @Test
    void ignoresMessagesFromThisNode() {
        final Integer id = createEntities(1).getFirst();
        cache(id);

        listener(updateHandler()).receive(fromNode(THIS_NODE, Action.UPDATE, id));

        assertTrue(isCached(id));
    }

    private static HibernateEntityUpdateTopicListener listener(final HibernateEntityEventHandlerMethod... handlers) {
        return listener(transactionManager, handlers);
    }

    private static HibernateEntityUpdateTopicListener listener(final PlatformTransactionManager transactionManager, final HibernateEntityEventHandlerMethod... handlers) {
        return new HibernateEntityUpdateTopicListener(mock(NrgEventServiceI.class), sessionFactory, transactionManager, THIS_NODE, Arrays.asList(handlers));
    }

    private static RecordingHandlerMethod updateHandler() {
        return new RecordingHandlerMethod(HibernateEntityEventHandlerMethod.Action.UPDATE);
    }

    private static HibernateEntityUpdateMessage updateFromOtherNode(final Integer id) {
        return fromOtherNode(Action.UPDATE, id);
    }

    private static HibernateEntityUpdateMessage fromOtherNode(final Action action, final Integer id) {
        return fromNode(OTHER_NODE, action, id);
    }

    private static HibernateEntityUpdateMessage fromNode(final String nodeId, final Action action, final Integer id) {
        return HibernateEntityUpdateMessage.builder()
                                           .originatingNodeId(nodeId)
                                           .timestamp("0")
                                           .action(action)
                                           .entityType(LargeObjectEntity.class.getName())
                                           .id(id)
                                           .build();
    }

    private static List<Integer> createEntities(final int count) {
        return transaction.execute(status -> IntStream.range(0, count)
                                                      .mapToObj(index -> {
                                                          final LargeObjectEntity entity = new LargeObjectEntity("payload-" + index);
                                                          sessionFactory.getCurrentSession().save(entity);
                                                          return entity.getId();
                                                      })
                                                      .collect(Collectors.toList()));
    }

    private static void cache(final Integer id) {
        transaction.executeWithoutResult(status -> sessionFactory.getCurrentSession().get(LargeObjectEntity.class, id));
        assertTrue(isCached(id));
    }

    private static boolean isCached(final Integer id) {
        return sessionFactory.getCache().containsEntity(LargeObjectEntity.class, id);
    }

    private static void terminateOtherConnections() throws SQLException {
        try (final Connection connection = POSTGRES.createConnection("");
             final Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()");
        }
    }

    private static Statistics getStatistics() {
        return sessionFactory.getStatistics();
    }
}
