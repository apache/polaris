/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.storage.aws;

import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;
import static org.apache.polaris.core.storage.aws.AwsSessionNameBuilder.buildSessionName;
import static org.apache.polaris.core.storage.aws.AwsSessionTagsBuilder.buildSessionTags;

import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.CachingStorageIntegration;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageUri;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.core.storage.aws.StsClientProvider.StsDestination;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Tag;

/** Credential vendor that supports generating AWS STS subscoped credentials. */
public class AwsCredentialsStorageIntegration
    extends CachingStorageIntegration<AwsStorageConfigurationInfo> {
  private final StsClientProvider stsClientProvider;
  private final Function<AwsStorageConfigurationInfo, Optional<AwsCredentialsProvider>>
      credentialsResolver;

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AwsCredentialsStorageIntegration.class);

  /** Test constructor — no cache, no request-scoped suppliers. */
  public AwsCredentialsStorageIntegration(
      StsClient fixedClient, AwsStorageConfigurationInfo storageConfig, RealmConfig realmConfig) {
    this((destination) -> fixedClient, config -> Optional.empty(), storageConfig, realmConfig);
  }

  /** Test constructor with credentials. */
  public AwsCredentialsStorageIntegration(
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> credentialsProvider,
      AwsStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig) {
    this(stsClientProvider, config -> credentialsProvider, storageConfig, realmConfig);
  }

  /** Constructor with credentials resolver (no cache). */
  public AwsCredentialsStorageIntegration(
      StsClientProvider stsClientProvider,
      Function<AwsStorageConfigurationInfo, Optional<AwsCredentialsProvider>> credentialsResolver,
      AwsStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig) {
    this(stsClientProvider, credentialsResolver, null, storageConfig, realmConfig);
  }

  /** Production constructor with cache and realm config. */
  public AwsCredentialsStorageIntegration(
      StsClientProvider stsClientProvider,
      Function<AwsStorageConfigurationInfo, Optional<AwsCredentialsProvider>> credentialsResolver,
      org.apache.polaris.core.storage.cache.StorageCredentialCache cache,
      AwsStorageConfigurationInfo storageConfig,
      RealmConfig realmConfig) {
    super(cache, realmConfig, storageConfig);
    this.stsClientProvider = stsClientProvider;
    this.credentialsResolver = credentialsResolver;
  }

  @Override
  protected StorageCredentialCacheKey buildCacheKey(
      @NonNull List<LocationGrant> grants,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context) {
    return buildCacheKey(
        readLocations(grants),
        listLocations(grants),
        writeLocations(grants),
        refreshEndpoint,
        context);
  }

  private static Set<String> readLocations(List<LocationGrant> grants) {
    return locationsFor(grants, PolarisStorageActions.READ, PolarisStorageActions.ALL);
  }

  private static Set<String> listLocations(List<LocationGrant> grants) {
    return locationsFor(grants, PolarisStorageActions.LIST, PolarisStorageActions.ALL);
  }

  private static Set<String> writeLocations(List<LocationGrant> grants) {
    return locationsFor(
        grants,
        PolarisStorageActions.WRITE,
        PolarisStorageActions.DELETE,
        PolarisStorageActions.ALL);
  }

  private static Set<String> locationsFor(
      List<LocationGrant> grants, PolarisStorageActions... wantedActions) {
    EnumSet<PolarisStorageActions> wanted = EnumSet.noneOf(PolarisStorageActions.class);
    Collections.addAll(wanted, wantedActions);
    return grants.stream()
        .filter(g -> g.actions().stream().anyMatch(wanted::contains))
        .flatMap(g -> g.locations().stream())
        .collect(Collectors.toSet());
  }

  private AwsStorageCredentialCacheKey buildCacheKey(
      @NonNull Set<String> readLocations,
      @NonNull Set<String> listLocations,
      @NonNull Set<String> writeLocations,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context) {
    RealmConfig realmConfig = realmConfig();
    String principalName = context.principalName().orElse("");
    String roleSessionName = resolveRoleSessionName(realmConfig, context, principalName);
    List<Tag> sessionTags = resolveSessionTags(realmConfig, context, principalName);
    return AwsStorageCredentialCacheKey.of(
        context.realm().orElse(""),
        storageConfig(),
        readLocations,
        listLocations,
        writeLocations,
        refreshEndpoint,
        roleSessionName,
        sessionTags,
        stsClientProvider,
        credentialsResolver,
        realmConfig);
  }

  private static String resolveRoleSessionName(
      RealmConfig realmConfig, CredentialVendingContext context, String principalName) {
    List<String> sessionNameFieldNames =
        realmConfig.getConfig(FeatureConfiguration.SESSION_NAME_FIELDS_IN_SUBSCOPED_CREDENTIAL);
    String sessionNamePrefix = AwsSessionNameBuilder.extractPrefix(sessionNameFieldNames);
    List<SessionNameField> enabledSessionNameFields =
        sessionNameFieldNames.stream()
            .filter(t -> !t.startsWith(AwsSessionNameBuilder.PREFIX_CONFIG_TOKEN))
            .map(SessionNameField::fromConfigName)
            .flatMap(Optional::stream)
            .toList();
    if (!enabledSessionNameFields.isEmpty()) {
      return buildSessionName(principalName, context, enabledSessionNameFields, sessionNamePrefix);
    }
    if (realmConfig.getConfig(
        FeatureConfiguration.INCLUDE_PRINCIPAL_NAME_IN_SUBSCOPED_CREDENTIAL)) {
      return AwsRoleSessionNameSanitizer.sanitize("polaris-" + principalName);
    }
    return AwsSessionNameBuilder.DEFAULT_SESSION_NAME;
  }

  private static List<Tag> resolveSessionTags(
      RealmConfig realmConfig, CredentialVendingContext context, String principalName) {
    List<String> sessionTagFieldNames =
        realmConfig.getConfig(FeatureConfiguration.SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL);
    if (sessionTagFieldNames.isEmpty() || context.equals(CredentialVendingContext.empty())) {
      return List.of();
    }
    Set<SessionTagField> enabledSessionTagFields =
        sessionTagFieldNames.stream()
            .map(SessionTagField::fromConfigName)
            .flatMap(Optional::stream)
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(SessionTagField.class)));
    return buildSessionTags(principalName, context, enabledSessionTagFields);
  }

  /** Mint a fresh {@link StorageAccessConfig} for the given AWS cache key. */
  static StorageAccessConfig compute(AwsStorageCredentialCacheKey key) {
    RealmConfig realmConfig = key.realmConfig();
    AwsStorageConfigurationInfo awsStorageConfig = key.storageConfig();
    int storageCredentialDurationSeconds =
        realmConfig.getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS);
    String region = awsStorageConfig.getRegion();
    StorageAccessConfig.Builder accessConfig = StorageAccessConfig.builder();
    String roleSessionName = key.roleSessionName();

    if (shouldUseSts(awsStorageConfig)) {
      AssumeRoleRequest.Builder request =
          AssumeRoleRequest.builder()
              .externalId(awsStorageConfig.getExternalId())
              .roleArn(awsStorageConfig.getRoleARN())
              .roleSessionName(roleSessionName)
              .policy(
                  policyString(
                          awsStorageConfig,
                          key.allowedReadLocations(),
                          key.allowedListLocations(),
                          key.allowedWriteLocations(),
                          region)
                      .toJson())
              .durationSeconds(storageCredentialDurationSeconds);

      List<Tag> sessionTags = key.sessionTags();
      if (!sessionTags.isEmpty()) {
        request.tags(sessionTags);
        // Mark all tags as transitive for role chaining support
        request.transitiveTagKeys(sessionTags.stream().map(Tag::key).toList());
      }

      key.credentialsResolver()
          .apply(awsStorageConfig)
          .ifPresent(cp -> request.overrideConfiguration(b -> b.credentialsProvider(cp)));

      @SuppressWarnings("resource")
      // Note: stsClientProvider returns "thin" clients that do not need closing
      StsClient stsClient =
          key.stsClientProvider()
              .stsClient(StsDestination.of(awsStorageConfig.getStsEndpointUri(), region));

      AssumeRoleResponse response = stsClient.assumeRole(request.build());
      accessConfig.put(StorageAccessProperty.AWS_KEY_ID, response.credentials().accessKeyId());
      accessConfig.put(
          StorageAccessProperty.AWS_SECRET_KEY, response.credentials().secretAccessKey());
      accessConfig.put(StorageAccessProperty.AWS_TOKEN, response.credentials().sessionToken());
      Optional.ofNullable(response.credentials().expiration())
          .ifPresent(
              i -> {
                accessConfig.put(
                    StorageAccessProperty.EXPIRATION_TIME, String.valueOf(i.toEpochMilli()));
                accessConfig.put(
                    StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS,
                    String.valueOf(i.toEpochMilli()));
              });
    }

    if (region != null) {
      accessConfig.put(StorageAccessProperty.CLIENT_REGION, region);
    }

    key.refreshCredentialsEndpoint()
        .ifPresent(
            endpoint ->
                accessConfig.put(StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT, endpoint));

    URI endpointUri = awsStorageConfig.getEndpointUri();
    if (endpointUri != null) {
      accessConfig.put(StorageAccessProperty.AWS_ENDPOINT, endpointUri.toString());
    }
    URI internalEndpointUri = awsStorageConfig.getInternalEndpointUri();
    if (internalEndpointUri != null) {
      accessConfig.putInternalProperty(
          StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), internalEndpointUri.toString());
    }

    if (Boolean.TRUE.equals(awsStorageConfig.getPathStyleAccess())) {
      accessConfig.put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS, Boolean.TRUE.toString());
    }

    if ("aws-us-gov".equals(awsStorageConfig.getAwsPartition()) && region == null) {
      throw new IllegalArgumentException(
          String.format(
              "AWS region must be set when using partition %s",
              awsStorageConfig.getAwsPartition()));
    }

    return accessConfig.build();
  }

  private static boolean shouldUseSts(AwsStorageConfigurationInfo storageConfig) {
    return !Boolean.TRUE.equals(storageConfig.getStsUnavailable());
  }

  private static boolean shouldUseKms(AwsStorageConfigurationInfo storageConfig) {
    return !Boolean.TRUE.equals(storageConfig.getKmsUnavailable());
  }

  /**
   * Generate an IamPolicy honoring per-action grants: {@code readLocations} get GetObject /
   * GetObjectVersion, {@code listLocations} get ListBucket with prefix conditions, {@code
   * writeLocations} get PutObject / DeleteObject. Buckets that appear in any set get
   * GetBucketLocation. If the resulting policy would have no statements (e.g. all three location
   * sets are empty and no KMS configuration applies), a single explicit {@code Deny *} statement is
   * emitted so we don't send an empty policy to AWS and inadvertently assume the role with full
   * privileges.
   */
  private static IamPolicy policyString(
      AwsStorageConfigurationInfo storageConfigurationInfo,
      Set<String> readLocations,
      Set<String> listLocations,
      Set<String> writeLocations,
      String region) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();
    IamStatement.Builder allowGetObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");
    Map<String, IamStatement.Builder> bucketListStatementBuilder = new HashMap<>();
    Map<String, IamStatement.Builder> bucketGetLocationStatementBuilder = new HashMap<>();

    String arnPrefix = arnPrefixForPartition(storageConfigurationInfo.getAwsPartition());
    String currentKmsKey = storageConfigurationInfo.getCurrentKmsKey();
    List<String> allowedKmsKeys = storageConfigurationInfo.getAllowedKmsKeys();

    readLocations.forEach(
        location -> {
          StorageUri uri = StorageUri.parse(location);
          String escapedObjectPrefix = escapeIamGlobLiteral(parseS3Path(uri));
          allowGetObjectStatementBuilder.addResource(
              IamResource.create(
                  arnPrefix + StorageUtil.concatFilePrefixes(escapedObjectPrefix, "*", "/")));
        });

    listLocations.forEach(
        location -> {
          StorageUri uri = StorageUri.parse(location);
          final var bucket = arnPrefix + uri.authority();
          String escapedListPrefix = escapeIamGlobLiteral(trimLeadingSlash(uri.rawPath()));
          bucketListStatementBuilder
              .computeIfAbsent(
                  bucket,
                  (String key) ->
                      IamStatement.builder()
                          .effect(IamEffect.ALLOW)
                          .addAction("s3:ListBucket")
                          .addResource(key))
              .addCondition(
                  IamConditionOperator.STRING_LIKE,
                  "s3:prefix",
                  StorageUtil.concatFilePrefixes(escapedListPrefix, "*", "/"));
        });

    Stream.of(readLocations, listLocations, writeLocations)
        .flatMap(Set::stream)
        .distinct()
        .forEach(
            location -> {
              final var bucket = arnPrefix + StorageUri.parse(location).authority();
              bucketGetLocationStatementBuilder.computeIfAbsent(
                  bucket,
                  key ->
                      IamStatement.builder()
                          .effect(IamEffect.ALLOW)
                          .addAction("s3:GetBucketLocation")
                          .addResource(key));
            });

    int statementCount = 0;
    boolean canWrite = !writeLocations.isEmpty();
    if (canWrite) {
      IamStatement.Builder allowPutObjectStatementBuilder =
          IamStatement.builder()
              .effect(IamEffect.ALLOW)
              .addAction("s3:PutObject")
              .addAction("s3:DeleteObject");
      writeLocations.forEach(
          location -> {
            StorageUri uri = StorageUri.parse(location);
            String escapedObjectPrefix = escapeIamGlobLiteral(parseS3Path(uri));
            allowPutObjectStatementBuilder.addResource(
                IamResource.create(
                    arnPrefix + StorageUtil.concatFilePrefixes(escapedObjectPrefix, "*", "/")));
          });
      policyBuilder.addStatement(allowPutObjectStatementBuilder.build());
      statementCount++;
    }
    if (shouldUseKms(storageConfigurationInfo)) {
      if (addKmsKeyPolicy(
          currentKmsKey,
          allowedKmsKeys,
          policyBuilder,
          canWrite,
          region,
          storageConfigurationInfo.getAwsAccountId())) {
        statementCount++;
      }
    }
    for (IamStatement.Builder statementBuilder : bucketListStatementBuilder.values()) {
      policyBuilder.addStatement(statementBuilder.build());
      statementCount++;
    }
    for (IamStatement.Builder statementBuilder : bucketGetLocationStatementBuilder.values()) {
      policyBuilder.addStatement(statementBuilder.build());
      statementCount++;
    }

    // Only emit the GetObject statement when there are read locations to bind it to; an
    // ALLOW statement with no Resource is rejected as a malformed policy by STS.
    if (!readLocations.isEmpty()) {
      policyBuilder.addStatement(allowGetObjectStatementBuilder.build());
      statementCount++;
    }
    if (statementCount == 0) {
      // Avoid sending an empty policy to STS, which would cause the assumed role to fall back
      // to its full privileges. An explicit deny-everything yields a well-formed but useless
      // credential.
      policyBuilder.addStatement(
          IamStatement.builder().effect(IamEffect.DENY).addAction("*").addResource("*").build());
    }
    return policyBuilder.build();
  }

  private static boolean addKmsKeyPolicy(
      String kmsKeyArn,
      List<String> allowedKmsKeys,
      IamPolicy.Builder policyBuilder,
      boolean canWrite,
      String region,
      String accountId) {

    boolean hasCurrentKey = kmsKeyArn != null;
    boolean hasAllowedKeys = hasAllowedKmsKeys(allowedKmsKeys);
    boolean isAwsS3 = region != null && accountId != null;

    // Nothing to do if no keys are configured and not AWS S3
    if (!hasCurrentKey && !hasAllowedKeys && !isAwsS3) {
      return false;
    }

    IamStatement.Builder allowKms = buildBaseKmsStatement(canWrite);

    if (hasCurrentKey) {
      addKmsKeyResource(kmsKeyArn, allowKms);
    }

    if (hasAllowedKeys) {
      addAllowedKmsKeyResources(allowedKmsKeys, allowKms);
    }

    // Only add wildcard KMS access for read-only operations on AWS S3 when no specific keys are
    // configured. This does not apply to services like Minio where region and accountId are not
    // available.
    boolean shouldAddWildcard = !hasCurrentKey && !hasAllowedKeys && !canWrite && isAwsS3;
    if (shouldAddWildcard) {
      addAllKeysResource(region, accountId, allowKms);
    }

    if (hasCurrentKey || hasAllowedKeys || shouldAddWildcard) {
      policyBuilder.addStatement(allowKms.build());
      return true;
    }
    return false;
  }

  private static IamStatement.Builder buildBaseKmsStatement(boolean canEncrypt) {
    IamStatement.Builder allowKms =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("kms:DescribeKey")
            .addAction("kms:Decrypt");

    if (canEncrypt) {
      allowKms
          .addAction("kms:Encrypt")
          .addAction("kms:GenerateDataKey")
          .addAction("kms:GenerateDataKeyWithoutPlaintext");
    }

    return allowKms;
  }

  private static void addKmsKeyResource(String kmsKeyArn, IamStatement.Builder allowKms) {
    if (kmsKeyArn != null) {
      LOGGER.debug("Adding KMS key policy for key {}", kmsKeyArn);
      allowKms.addResource(IamResource.create(kmsKeyArn));
    }
  }

  private static boolean hasAllowedKmsKeys(List<String> allowedKmsKeys) {
    return allowedKmsKeys != null && !allowedKmsKeys.isEmpty();
  }

  private static void addAllowedKmsKeyResources(
      List<String> allowedKmsKeys, IamStatement.Builder allowKms) {
    allowedKmsKeys.forEach(
        keyArn -> {
          LOGGER.debug("Adding allowed KMS key policy for key {}", keyArn);
          allowKms.addResource(IamResource.create(keyArn));
        });
  }

  private static void addAllKeysResource(
      String region, String accountId, IamStatement.Builder allowKms) {
    String allKeysArn = arnKeyAll(region, accountId);
    allowKms.addResource(IamResource.create(allKeysArn));
    LOGGER.debug("Adding KMS key policy for all keys in account {}", accountId);
  }

  private static String arnKeyAll(String region, String accountId) {
    return String.format("arn:aws:kms:%s:%s:key/*", region, accountId);
  }

  private static String arnPrefixForPartition(String awsPartition) {
    return String.format("arn:%s:s3:::", awsPartition != null ? awsPartition : "aws");
  }

  private static @NonNull String parseS3Path(StorageUri uri) {
    String bucket = uri.authority();
    String path = trimLeadingSlash(uri.rawPath());
    return String.join("/", bucket, path);
  }

  /**
   * Escapes IAM pattern characters that must remain literal inside object resource ARNs and {@link
   * IamConditionOperator#STRING_LIKE StringLike} conditions.
   */
  @VisibleForTesting
  static @NonNull String escapeIamGlobLiteral(String value) {
    StringBuilder escaped = new StringBuilder(value.length() + 8);
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '*' -> escaped.append("${*}");
        case '?' -> escaped.append("${?}");
        case '$' -> escaped.append("${$}");
        default -> escaped.append(c);
      }
    }
    return escaped.toString();
  }

  private static @NonNull String trimLeadingSlash(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return path;
  }
}
