package com.bnpparibas.nats.spring.autoconfigure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "bnpp.nats")
public class BnppNatsProperties {
    private boolean enabled = true;
    private boolean autoStart = true;
    private boolean reconnectOnConnect = true;
    private boolean createTopology = false;
    private boolean validateTopology = true;
    private boolean failOnInvalidTopology = true;
    private List<String> servers = new ArrayList<>(List.of("nats://localhost:4222"));
    private String connectionName = "bnpp-nats";
    private Duration connectionTimeout = Duration.ofSeconds(5);
    private Duration socketWriteTimeout = Duration.ofSeconds(30);
    private Duration pingInterval = Duration.ofSeconds(30);
    private Duration reconnectWait = Duration.ofSeconds(2);
    private int maxReconnects = -1;
    private long reconnectBufferSize = 8 * 1024 * 1024;
    private SubjectValidation subjectValidation = SubjectValidation.LENIENT;
    private boolean noEcho = true;
    private boolean useDispatcherWithExecutor = true;
    private Auth auth = new Auth();
    private Tls tls = new Tls();
    private Executor executor = new Executor();
    private JetStream jetStream = new JetStream();
    private Topology topology = new Topology();

    public enum SubjectValidation { LENIENT, NONE, STRICT }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public boolean isAutoStart() { return autoStart; }
    public void setAutoStart(boolean autoStart) { this.autoStart = autoStart; }
    public boolean isReconnectOnConnect() { return reconnectOnConnect; }
    public void setReconnectOnConnect(boolean reconnectOnConnect) { this.reconnectOnConnect = reconnectOnConnect; }
    public boolean isCreateTopology() { return createTopology; }
    public void setCreateTopology(boolean createTopology) { this.createTopology = createTopology; }
    public boolean isValidateTopology() { return validateTopology; }
    public void setValidateTopology(boolean validateTopology) { this.validateTopology = validateTopology; }
    public boolean isFailOnInvalidTopology() { return failOnInvalidTopology; }
    public void setFailOnInvalidTopology(boolean failOnInvalidTopology) { this.failOnInvalidTopology = failOnInvalidTopology; }
    public List<String> getServers() { return servers; }
    public void setServers(List<String> servers) { this.servers = servers; }
    public String getConnectionName() { return connectionName; }
    public void setConnectionName(String connectionName) { this.connectionName = connectionName; }
    public Duration getConnectionTimeout() { return connectionTimeout; }
    public void setConnectionTimeout(Duration connectionTimeout) { this.connectionTimeout = connectionTimeout; }
    public Duration getSocketWriteTimeout() { return socketWriteTimeout; }
    public void setSocketWriteTimeout(Duration socketWriteTimeout) { this.socketWriteTimeout = socketWriteTimeout; }
    public Duration getPingInterval() { return pingInterval; }
    public void setPingInterval(Duration pingInterval) { this.pingInterval = pingInterval; }
    public Duration getReconnectWait() { return reconnectWait; }
    public void setReconnectWait(Duration reconnectWait) { this.reconnectWait = reconnectWait; }
    public int getMaxReconnects() { return maxReconnects; }
    public void setMaxReconnects(int maxReconnects) { this.maxReconnects = maxReconnects; }
    public long getReconnectBufferSize() { return reconnectBufferSize; }
    public void setReconnectBufferSize(long reconnectBufferSize) { this.reconnectBufferSize = reconnectBufferSize; }
    public SubjectValidation getSubjectValidation() { return subjectValidation; }
    public void setSubjectValidation(SubjectValidation subjectValidation) { this.subjectValidation = subjectValidation; }
    public boolean isNoEcho() { return noEcho; }
    public void setNoEcho(boolean noEcho) { this.noEcho = noEcho; }
    public boolean isUseDispatcherWithExecutor() { return useDispatcherWithExecutor; }
    public void setUseDispatcherWithExecutor(boolean useDispatcherWithExecutor) { this.useDispatcherWithExecutor = useDispatcherWithExecutor; }
    public Auth getAuth() { return auth; }
    public void setAuth(Auth auth) { this.auth = auth; }
    public Tls getTls() { return tls; }
    public void setTls(Tls tls) { this.tls = tls; }
    public Executor getExecutor() { return executor; }
    public void setExecutor(Executor executor) { this.executor = executor; }
    public JetStream getJetStream() { return jetStream; }
    public void setJetStream(JetStream jetStream) { this.jetStream = jetStream; }
    public Topology getTopology() { return topology; }
    public void setTopology(Topology topology) { this.topology = topology; }

    public static class Auth {
        private String username;
        private String password;
        private String token;
        private String credentialsPath;
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getToken() { return token; }
        public void setToken(String token) { this.token = token; }
        public String getCredentialsPath() { return credentialsPath; }
        public void setCredentialsPath(String credentialsPath) { this.credentialsPath = credentialsPath; }
    }

    public static class Tls {
        private boolean secure = false;
        private String trustStorePath;
        private String trustStorePassword;
        private String keyStorePath;
        private String keyStorePassword;
        private String algorithm;
        private boolean tlsFirst = false;
        public boolean isSecure() { return secure; }
        public void setSecure(boolean secure) { this.secure = secure; }
        public String getTrustStorePath() { return trustStorePath; }
        public void setTrustStorePath(String trustStorePath) { this.trustStorePath = trustStorePath; }
        public String getTrustStorePassword() { return trustStorePassword; }
        public void setTrustStorePassword(String trustStorePassword) { this.trustStorePassword = trustStorePassword; }
        public String getKeyStorePath() { return keyStorePath; }
        public void setKeyStorePath(String keyStorePath) { this.keyStorePath = keyStorePath; }
        public String getKeyStorePassword() { return keyStorePassword; }
        public void setKeyStorePassword(String keyStorePassword) { this.keyStorePassword = keyStorePassword; }
        public String getAlgorithm() { return algorithm; }
        public void setAlgorithm(String algorithm) { this.algorithm = algorithm; }
        public boolean isTlsFirst() { return tlsFirst; }
        public void setTlsFirst(boolean tlsFirst) { this.tlsFirst = tlsFirst; }
    }

    public static class Executor {
        private boolean virtualThreads = true;
        private int platformThreads = Runtime.getRuntime().availableProcessors();
        public boolean isVirtualThreads() { return virtualThreads; }
        public void setVirtualThreads(boolean virtualThreads) { this.virtualThreads = virtualThreads; }
        public int getPlatformThreads() { return platformThreads; }
        public void setPlatformThreads(int platformThreads) { this.platformThreads = platformThreads; }
    }

    public static class JetStream {
        private boolean autoCreateStreamOnPublishFailure = false;
        public boolean isAutoCreateStreamOnPublishFailure() { return autoCreateStreamOnPublishFailure; }
        public void setAutoCreateStreamOnPublishFailure(boolean autoCreateStreamOnPublishFailure) {
            this.autoCreateStreamOnPublishFailure = autoCreateStreamOnPublishFailure;
        }
    }

    public static class Topology {
        private List<Stream> streams = new ArrayList<>();
        private List<Consumer> consumers = new ArrayList<>();
        public List<Stream> getStreams() { return streams; }
        public void setStreams(List<Stream> streams) { this.streams = streams; }
        public List<Consumer> getConsumers() { return consumers; }
        public void setConsumers(List<Consumer> consumers) { this.consumers = consumers; }
    }

    public static class Stream {
        private String name;
        private List<String> subjects = new ArrayList<>();
        private String storageType = "File";
        private String retentionPolicy = "Limits";
        private Integer replicas;
        private Long maxMessages;
        private Long maxBytes;
        private Duration maxAge;
        private Duration duplicateWindow;
        private Map<String, String> metadata = new HashMap<>();
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public List<String> getSubjects() { return subjects; }
        public void setSubjects(List<String> subjects) { this.subjects = subjects; }
        public String getStorageType() { return storageType; }
        public void setStorageType(String storageType) { this.storageType = storageType; }
        public String getRetentionPolicy() { return retentionPolicy; }
        public void setRetentionPolicy(String retentionPolicy) { this.retentionPolicy = retentionPolicy; }
        public Integer getReplicas() { return replicas; }
        public void setReplicas(Integer replicas) { this.replicas = replicas; }
        public Long getMaxMessages() { return maxMessages; }
        public void setMaxMessages(Long maxMessages) { this.maxMessages = maxMessages; }
        public Long getMaxBytes() { return maxBytes; }
        public void setMaxBytes(Long maxBytes) { this.maxBytes = maxBytes; }
        public Duration getMaxAge() { return maxAge; }
        public void setMaxAge(Duration maxAge) { this.maxAge = maxAge; }
        public Duration getDuplicateWindow() { return duplicateWindow; }
        public void setDuplicateWindow(Duration duplicateWindow) { this.duplicateWindow = duplicateWindow; }
        public Map<String, String> getMetadata() { return metadata; }
        public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }
    }

    public static class Consumer {
        private String stream;
        private String durable;
        private String name;
        private List<String> filterSubjects = new ArrayList<>();
        private String ackPolicy = "Explicit";
        private String deliverPolicy = "All";
        private Duration ackWait;
        private Long maxDeliver;
        private Long maxAckPending;
        private Map<String, String> metadata = new HashMap<>();
        public String getStream() { return stream; }
        public void setStream(String stream) { this.stream = stream; }
        public String getDurable() { return durable; }
        public void setDurable(String durable) { this.durable = durable; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public List<String> getFilterSubjects() { return filterSubjects; }
        public void setFilterSubjects(List<String> filterSubjects) { this.filterSubjects = filterSubjects; }
        public String getAckPolicy() { return ackPolicy; }
        public void setAckPolicy(String ackPolicy) { this.ackPolicy = ackPolicy; }
        public String getDeliverPolicy() { return deliverPolicy; }
        public void setDeliverPolicy(String deliverPolicy) { this.deliverPolicy = deliverPolicy; }
        public Duration getAckWait() { return ackWait; }
        public void setAckWait(Duration ackWait) { this.ackWait = ackWait; }
        public Long getMaxDeliver() { return maxDeliver; }
        public void setMaxDeliver(Long maxDeliver) { this.maxDeliver = maxDeliver; }
        public Long getMaxAckPending() { return maxAckPending; }
        public void setMaxAckPending(Long maxAckPending) { this.maxAckPending = maxAckPending; }
        public Map<String, String> getMetadata() { return metadata; }
        public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }
    }
}
