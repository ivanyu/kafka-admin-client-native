package me.ivanyu.kafka_admin_client_native;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
