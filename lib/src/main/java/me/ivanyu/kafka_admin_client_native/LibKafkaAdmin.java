package me.ivanyu.kafka_admin_client_native;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.ObjectHandle;
import org.graalvm.nativeimage.ObjectHandles;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.struct.CField;
import org.graalvm.nativeimage.c.struct.CStruct;
import org.graalvm.nativeimage.c.struct.SizeOf;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CConst;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.PointerBase;
import org.graalvm.word.UnsignedWord;
import org.graalvm.word.WordFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@CContext(LibKafkaAdmin.Directives.class)
public class LibKafkaAdmin {
    private static Logger LOGGER = LoggerFactory.getLogger(LibKafkaAdmin.class);

    static class Directives implements CContext.Directives {
        @Override
        public List<String> getHeaderFiles() {
            return Collections.singletonList("\"" + Path.of(System.getProperty("me.ivanyu.headerfile")).toAbsolutePath() + "\"");
        }
    }

    @CStruct("key_value_t")
    public interface KeyValue extends PointerBase {
        @CField("key")
        CCharPointer key();

        @CField("value")
        CCharPointer value();

        KeyValue read(int index);
    }

    @CStruct("node_t")
    public interface NodeExt extends PointerBase {
        @CField("id")
        void id(int value);

        @CField("host")
        CCharPointer host();

        @CField("host")
        void host(CCharPointer value);

        @CField("port")
        void port(int value);

        @CField("rack")
        void rack(CCharPointer value);

        @CField("rack")
        CCharPointer rack();

        NodeExt read();

        NodeExt read(int index);
    }

    private static void freeNodeExt(final NodeExt nodeExt) {
        UnmanagedMemory.free(nodeExt.host());
        UnmanagedMemory.free(nodeExt.rack());
    }

    @CStruct("describe_cluster_result_t")
    public interface DescribeClusterResultExt extends PointerBase {
        @CField("num_nodes")
        int numNodes();

        @CField("num_nodes")
        void numNodes(int value);

        @CField("nodes")
        NodeExt nodes();

        @CField("nodes")
        void nodes(NodeExt value);

        @CField("controller")
        NodeExt controller();

        @CField("controller")
        void controller(NodeExt value);

        @CField("cluster_id")
        void clusterId(CCharPointer value);

        @CField("cluster_id")
        CCharPointer clusterId();

        @CField("num_authorized_operations")
        int numAuthorizedOperations();

        @CField("num_authorized_operations")
        void numAuthorizedOperations(int value);

        @CField("authorized_operations")
        CCharPointer authorizedOperations();

        @CField("authorized_operations")
        void authorizedOperations(CCharPointer value);
    }

    private static void freeDescribeClusterResultExt(final DescribeClusterResultExt resultExt) {
        for (int i = 0; i < resultExt.numNodes(); i++) {
            freeNodeExt(resultExt.nodes().read(i));
        }
        UnmanagedMemory.free(resultExt.nodes());

        freeNodeExt(resultExt.controller());
        UnmanagedMemory.free(resultExt.controller());

        UnmanagedMemory.free(resultExt.clusterId());

        UnmanagedMemory.free(resultExt.authorizedOperations());
    }

    @CStruct("new_topic_t")
    public interface NewTopicExt extends PointerBase {
        @CField("name")
        CCharPointer name();

        @CField("num_partitions")
        int numPartitions();

        @CField("replication_factor")
        short replicationFactor();

        NewTopicExt read(int index);
    }

    @CStruct("create_topic_result_t")
    public interface CreateTopicResultExt extends PointerBase {
        @CField("topic")
        CCharPointer topic();

        @CField("topic")
        void topic(CCharPointer value);

        @CField("error")
        void error(CCharPointer value);

        @CField("error")
        CCharPointer error();

        @CField("uuid")
        void uuid(CCharPointer value);

        @CField("uuid")
        CCharPointer uuid();

        @CField("num_partitions")
        void numPartitions(int value);

        @CField("replication_factor")
        void replicationFactor(int value);

        CreateTopicResultExt read(int index);
    }

    private static void freeCreateTopicResultExt(final CreateTopicResultExt createTopicResultExt) {
        UnmanagedMemory.free(createTopicResultExt.topic());
        UnmanagedMemory.free(createTopicResultExt.error());
        UnmanagedMemory.free(createTopicResultExt.uuid());
    }

    @CStruct("create_topics_result_t")
    public interface CreateTopicsResultExt extends PointerBase {
        @CField("num_topics")
        int numTopics();

        @CField("num_topics")
        void numTopics(int value);

        @CField("topics")
        CreateTopicResultExt topics();

        @CField("topics")
        void topics(CreateTopicResultExt value);
    }

    private static void freeCreateTopicsResultExt(final CreateTopicsResultExt createTopicsResultExt) {
        for (int i = 0; i < createTopicsResultExt.numTopics(); i++) {
            freeCreateTopicResultExt(createTopicsResultExt.topics().read(i));
        }
        UnmanagedMemory.free(createTopicsResultExt.topics());
    }

    @CEntryPoint(name = "create_admin_client")
    public static ObjectHandle createAdminClient(final IsolateThread thread,
                                                 final int kvCount,
                                                 @CConst final KeyValue kvs) {
        try {
            final HashMap<String, Object> config = new HashMap<>();
            for (int i = 0; i < kvCount; i++) {
                final KeyValue kv = kvs.read(i);
                String key = CTypeConversion.toJavaString(kv.key());
                String value = CTypeConversion.toJavaString(kv.value());
                config.put(key, value);
            }
            final AdminClient client = KafkaAdminClient.create(config);
            return ObjectHandles.getGlobal().create(client);
        } catch (final Exception e) {
            LOGGER.error("Error creating admin client", e);
            return WordFactory.nullPointer();
        }
    }

    @CEntryPoint(name = "describe_cluster")
    @CConst
    public static DescribeClusterResultExt describeCluster(@CConst final IsolateThread thread,
                                                           @CConst final ObjectHandle handle) {
        final AdminClient client = ObjectHandles.getGlobal().get(handle);
        if (client == null) {
            LOGGER.error("Invalid handler, ignoring");
            return WordFactory.nullPointer();
        }

        final DescribeClusterResultExt result = UnmanagedMemory.malloc(SizeOf.get(DescribeClusterResultExt.class));

        final Collection<Node> nodes;
        final Node controller;
        final String clusterId;
        final Set<AclOperation> authorizedOperations;
        try {
            final var resultInt = client.describeCluster();
            nodes = resultInt.nodes().get();
            controller = resultInt.controller().get();
            clusterId = resultInt.clusterId().get();
            authorizedOperations = resultInt.authorizedOperations().get();
        } catch (final InterruptedException | ExecutionException e) {
            LOGGER.error("Error describing cluster", e);
            return WordFactory.nullPointer();
        }

        final NodeExt nodesExt = UnmanagedMemory.malloc(SizeOf.get(NodeExt.class) * nodes.size());
        result.numNodes(nodes.size());
        int i = 0;
        for (final Node node : nodes) {
            final NodeExt nodeExt = nodesExt.read(i);
            i += 1;
            setNodeExt(nodeExt, node);
        }
        result.nodes(nodesExt);

        final NodeExt controllerExt = UnmanagedMemory.malloc(SizeOf.get(NodeExt.class));
        setNodeExt(controllerExt, controller);
        result.controller(controllerExt);

        result.clusterId(toCStringUnmanaged(clusterId));

        if (authorizedOperations != null && !authorizedOperations.isEmpty()) {
            final CCharPointer aclOperationsExt = UnmanagedMemory.malloc(SizeOf.get(CCharPointer.class) * nodes.size());
            result.numAuthorizedOperations(authorizedOperations.size());
            i = 0;
            for (final AclOperation authorizedOperation : authorizedOperations) {
                aclOperationsExt.write(i, authorizedOperation.code());
                i += 1;
            }
            result.authorizedOperations(aclOperationsExt);
        } else {
            result.numAuthorizedOperations(0);
            result.authorizedOperations(WordFactory.nullPointer());
        }

        return result;
    }

    @CEntryPoint(name = "free_describe_cluster_result")
    public static void freeDescribeClusterResult(@CConst final IsolateThread thread,
                                                 @CConst final DescribeClusterResultExt describeClusterResult) {
        freeDescribeClusterResultExt(describeClusterResult);
    }

    private static void setNodeExt(final NodeExt nodeExt, final Node node) {
        nodeExt.id(node.id());
        nodeExt.host(toCStringUnmanaged(node.host()));
        nodeExt.port(node.port());
        nodeExt.rack(toCStringUnmanaged(node.rack()));
    }

    @CEntryPoint(name = "create_topics")
    public static CreateTopicsResultExt createTopics(@CConst final IsolateThread thread,
                                                     @CConst final ObjectHandle handle,
                                                     final int numNewTopics,
                                                     @CConst final NewTopicExt newTopics) {
        final AdminClient client = ObjectHandles.getGlobal().get(handle);
        if (client == null) {
            LOGGER.error("Invalid handler, ignoring");
            return WordFactory.nullPointer();
        }

        final ArrayList<NewTopic> newTopicsInt = new ArrayList<>(numNewTopics);
        for (int i = 0; i < numNewTopics; i++) {
            final NewTopicExt newTopicExt = newTopics.read(i);
            final Optional<Integer> numPartitions = newTopicExt.numPartitions() < 0
                ? Optional.empty()
                : Optional.of(newTopicExt.numPartitions());
            final Optional<Short> replicationFactor = newTopicExt.replicationFactor() < 0
                ? Optional.empty()
                : Optional.of(newTopicExt.replicationFactor());

            final NewTopic newTopic = new NewTopic(
                CTypeConversion.toJavaString(newTopicExt.name()),
                numPartitions,
                replicationFactor
            );
            newTopicsInt.add(newTopic);
        }

        // TODO support CreateTopicsOptions
        final CreateTopicsResult result = client.createTopics(newTopicsInt);

        final int numTopics = result.values().size();
        final CreateTopicsResultExt createTopicsResultExt = UnmanagedMemory.malloc(SizeOf.get(CreateTopicsResultExt.class));
        final CreateTopicResultExt createTopicResultExtArray = UnmanagedMemory.malloc(SizeOf.get(CreateTopicResultExt.class) * numTopics);
        int i = 0;
        for (final var entry : result.values().entrySet()) {
            final CreateTopicResultExt createTopicResultExt = createTopicResultExtArray.read(i);
            i += 1;

            final String topic = entry.getKey();
            createTopicResultExt.topic(toCStringUnmanaged(topic));
            try {
                final CCharPointer uuid = toCStringUnmanaged(result.topicId(topic).get().toString());
                final int numPartitions = result.numPartitions(topic).get();
                final int replicationFactor = result.replicationFactor(topic).get();

                createTopicResultExt.uuid(uuid);
                createTopicResultExt.numPartitions(numPartitions);
                createTopicResultExt.replicationFactor(replicationFactor);
            } catch (final Exception e) {
                createTopicResultExt.error(toCStringUnmanaged(e.getMessage()));
            }
        }
        createTopicsResultExt.numTopics(numTopics);
        createTopicsResultExt.topics(createTopicResultExtArray);
        return createTopicsResultExt;
    }

    @CEntryPoint(name = "free_create_topics_result")
    public static void freeCreateTopicsResult(@CConst final IsolateThread thread,
                                              @CConst final CreateTopicsResultExt createTopicsResultExt) {
        freeCreateTopicsResultExt(createTopicsResultExt);
    }

    private static CCharPointer toCStringUnmanaged(final String string) {
        if (string == null) {
            return WordFactory.nullPointer();
        }
        final Charset charset = StandardCharsets.UTF_8;
        final byte[] bytes = string.getBytes(charset);
        final UnsignedWord size = WordFactory.unsigned(bytes.length + 1);
        final CCharPointer result = UnmanagedMemory.malloc(size);
        CTypeConversion.toCString(string, charset, result, size);
        return result;
    }

    @CEntryPoint(name = "delete_admin_client")
    public static void deleteAdminClient(@CConst final IsolateThread thread, @CConst final ObjectHandle handle) {
        final ObjectHandles handles = ObjectHandles.getGlobal();
        final AdminClient client = handles.get(handle);
        if (client != null) {
            client.close();
            handles.destroy(handle);
        } else {
            LOGGER.error("Invalid handler, ignoring");
        }
    }
}
