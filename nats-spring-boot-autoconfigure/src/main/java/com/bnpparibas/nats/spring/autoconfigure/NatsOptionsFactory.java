package com.bnpparibas.nats.spring.autoconfigure;

import io.nats.client.Options;

import java.security.NoSuchAlgorithmException;

final class NatsOptionsFactory {
    private NatsOptionsFactory() {
    }

    static Options from(BnppNatsProperties properties) {
        try {
            Options.Builder builder = Options.builder()
                    .servers(properties.getServers().toArray(String[]::new))
                    .connectionName(properties.getConnectionName())
                    .connectionTimeout(properties.getConnectionTimeout())
                    .socketWriteTimeout(properties.getSocketWriteTimeout())
                    .pingInterval(properties.getPingInterval())
                    .reconnectWait(properties.getReconnectWait())
                    .maxReconnects(properties.getMaxReconnects())
                    .reconnectBufferSize(properties.getReconnectBufferSize());

            if (properties.isNoEcho()) {
                builder.noEcho();
            }
            if (properties.isUseDispatcherWithExecutor()) {
                builder.useDispatcherWithExecutor();
            }
            switch (properties.getSubjectValidation()) {
                case NONE -> builder.noSubjectValidation();
                case STRICT -> builder.strictSubjectValidation();
                case LENIENT -> { }
            }
            applyAuth(builder, properties.getAuth());
            applyTls(builder, properties.getTls());
            return builder.build();
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to build NATS options", ex);
        }
    }

    private static void applyAuth(Options.Builder builder, BnppNatsProperties.Auth auth) {
        if (hasText(auth.getCredentialsPath())) {
            builder.credentialPath(auth.getCredentialsPath());
        } else if (hasText(auth.getToken())) {
            builder.token(auth.getToken());
        } else if (hasText(auth.getUsername())) {
            builder.userInfo(auth.getUsername(), auth.getPassword() == null ? "" : auth.getPassword());
        }
    }

    private static void applyTls(Options.Builder builder, BnppNatsProperties.Tls tls) throws NoSuchAlgorithmException {
        if (tls.isSecure()) {
            builder.secure();
        }
        if (hasText(tls.getTrustStorePath())) {
            builder.truststorePath(tls.getTrustStorePath());
        }
        if (hasText(tls.getTrustStorePassword())) {
            builder.truststorePassword(tls.getTrustStorePassword().toCharArray());
        }
        if (hasText(tls.getKeyStorePath())) {
            builder.keystorePath(tls.getKeyStorePath());
        }
        if (hasText(tls.getKeyStorePassword())) {
            builder.keystorePassword(tls.getKeyStorePassword().toCharArray());
        }
        if (hasText(tls.getAlgorithm())) {
            builder.tlsAlgorithm(tls.getAlgorithm());
        }
        if (tls.isTlsFirst()) {
            builder.tlsFirst();
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
