You are a senior Java/Spring Boot architect. Design and implement a standalone,
reusable "Springbrook Workflow Tracker" — a generic Kafka-based workflow state
tracking engine deployable as an independent service per workflow using Helm on GCP.

═══════════════════════════════════════════════════════════════
BUSINESS CONTEXT
═══════════════════════════════════════════════════════════════

An existing event-driven pipeline processes financial "Books" end-to-end
across two legs:

Leg 1 — Inbound (Upstream → Power Apps):
  Upstream → [Source Adapter] → [Validator] → [Transformer]
           → [Persistence Layer] → [Consumer Adapter] → Power Apps

Leg 2 — Outbound (Power Apps recertification → HMS):
  Power Apps → [Recert Adapter] → [Transformer] → [Acceptance Layer]
             → [Inbound Adapter] → HMS

Each component publishes to a dedicated Kafka topic.
Users recertify Books in Power Apps; recertified responses flow back via Leg 2.
The goal is to passively track every Book's state across the entire pipeline
WITHOUT modifying any existing service.

═══════════════════════════════════════════════════════════════
CORE DESIGN PRINCIPLES
═══════════════════════════════════════════════════════════════

1. REUSABLE IMAGE
   One Docker image published to GCR. Any team deploys their own instance
   by providing their own Helm values — no code change, no new image build.

2. TWO-FILE CONFIGURATION MODEL (critical)
   Every deployment has exactly two configuration files:

   a) values.yaml — Helm / infrastructure config
      - Docker image repo and tag
      - Kafka bootstrap servers and credentials
      - PostgreSQL host, port, DB name, schema, credentials
      - Replica count, HPA settings, ingress, service type
      - All sensitive values injected as secrets

   b) runtime-application.yaml — Application / workflow business config
      - Workflow ID, description, entityIdField
      - Complete state definitions for this workflow
      - Kafka topic → state mappings
      - Optional Groovy pre-transition script reference per topic
      - Scripts mount path
      - Dead letter retry threshold
      Mounted into the pod as a ConfigMap at /app/config/
      Loaded by Spring Boot via:
        SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/runtime-application.yaml

3. GROOVY PRE-TRANSITION HOOKS
   Each Kafka topic can optionally reference a Groovy script.
   Scripts are stored in helm/scripts/, rendered into a ConfigMap,
   and mounted at /app/scripts/ inside the pod.
   The engine executes the script BEFORE updating state.
   Scripts receive: payload (Map), currentState (String), entityId (String),
   StateDecision (class reference).
   Scripts must return a StateDecision.

4. PASSIVE KAFKA CONSUMER
   This service only CONSUMES — never produces.
   It uses a unique consumer group ID per topic:
     {consumerGroupPrefix}-{workflowId}-{topicName}
   Existing pipeline consumers are NEVER affected.

5. IDEMPOTENT STATE TRANSITIONS
   Receiving the same Kafka message twice must never cause errors or
   duplicate state updates.

═══════════════════════════════════════════════════════════════
CONFIGURATION FILE SPECIFICATIONS
═══════════════════════════════════════════════════════════════

── values.yaml (complete example) ─────────────────────────────

image:
  repository: gcr.io/your-project/workflow-tracker
  tag: "1.0.0"
  pullPolicy: IfNotPresent

replicaCount: 2

service:
  type: ClusterIP
  port: 8080

kafka:
  bootstrapServers: "kafka-broker:9092"
  consumerGroupPrefix: "workflow-tracker"
  securityProtocol: "SASL_SSL"
  saslMechanism: "PLAIN"

database:
  host: "postgres-host"
  port: 5432
  name: "springbrook_tracker_db"
  schema: "springbrook_workflow"
  username: "tracker_user"

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 5
  cpuThreshold: 70

ingress:
  enabled: true
  host: "springbrook-tracker.your-domain.com"

secrets:
  dbPassword: ""
  kafkaPassword: ""

── runtime-application.yaml (complete example) ─────────────────

workflow:
  id: "SPRINGBROOK_BOOK_FLOW"
  description: "Springbrook end-to-end book processing"
  entityIdField: "bookId"
  deadLetterAfterRetries: 3

  states:
    - name: "RECEIVED"
    - name: "VALIDATED"
    - name: "VALIDATION_FAILED"
    - name: "TRANSFORMED"
    - name: "PERSISTED"
    - name: "SENT_TO_POWERAPP"
    - name: "PENDING_RECERTIFICATION"
    - name: "RECERTIFIED"
    - name: "RECERT_REJECTED"
    - name: "SENT_TO_HMS"
    - name: "COMPLETED"
    - name: "FAILED"
    - name: "DEAD_LETTERED"

  topics:
    - topicName: "source.adapter.output"
      mapsToState: "RECEIVED"

    - topicName: "validator.output"
      mapsToState: "VALIDATED"
      preTransitionScript: "validator-hook.groovy"

    - topicName: "transformer.output"
      mapsToState: "TRANSFORMED"
      preTransitionScript: "transformer-hook.groovy"

    - topicName: "persistence.output"
      mapsToState: "PERSISTED"

    - topicName: "consumer.adapter.output"
      mapsToState: "SENT_TO_POWERAPP"

    - topicName: "recert.adapter.output"
      mapsToState: "RECERTIFIED"
      preTransitionScript: "recert-hook.groovy"

    - topicName: "hms.adapter.output"
      mapsToState: "SENT_TO_HMS"

  groovyScripts:
    scriptsMountPath: "/app/scripts"

═══════════════════════════════════════════════════════════════
GROOVY HOOK SPECIFICATION
═══════════════════════════════════════════════════════════════

StateDecision contract (Java — defined in engine, exposed to scripts):

  StateDecision.proceed()
    → continue with configured mapsToState

  StateDecision.skip(String reason)
    → do not update state, log reason

  StateDecision.override(String newState)
    → update state to newState instead of configured mapsToState

Variables injected into every script:
  payload        Map<String, Object>   full Kafka message as parsed map
  currentState   String                entity's current persisted state
  entityId       String                extracted entity ID
  StateDecision  class reference       for calling factory methods above

Example scripts:

  // validator-hook.groovy
  if (payload.status == 'ERROR') {
      return StateDecision.override('VALIDATION_FAILED')
  }
  return StateDecision.proceed()

  // recert-hook.groovy
  if (currentState == 'SENT_TO_POWERAPP' && payload.action == 'REJECT') {
      return StateDecision.override('RECERT_REJECTED')
  }
  if (currentState == 'SENT_TO_POWERAPP' && payload.action == 'APPROVE') {
      return StateDecision.proceed()
  }
  return StateDecision.skip('Unexpected recert action: ' + payload.action)

═══════════════════════════════════════════════════════════════
TECH STACK
═══════════════════════════════════════════════════════════════

- Java 21
- Spring Boot 3.x
- Spring Kafka (consumer only — no KafkaTemplate, no producing)
- Spring Data JPA
- PostgreSQL
- Groovy JSR-223 (groovy-jsr223 dependency — for script execution)
- Lombok
- Maven
- Docker (multi-stage)
- Helm 3 (GCP deployment)

═══════════════════════════════════════════════════════════════
DELIVERABLES
═══════════════════════════════════════════════════════════════

Implement ALL of the following:

── 1. PROJECT STRUCTURE ────────────────────────────────────────
Complete Maven project layout with all packages:
  com.springbrook.tracker
    ├── config          (Spring config classes)
    ├── domain          (JPA entities, enums)
    ├── kafka           (listener registrar, message handler)
    ├── groovy          (script engine, StateDecision)
    ├── service         (WorkflowStateService)
    ├── repository      (JPA repositories)
    ├── api             (REST controllers, DTOs)
    └── properties      (config binding classes)

── 2. CONFIGURATION BINDING ────────────────────────────────────
WorkflowProperties.java
  @ConfigurationProperties(prefix = "workflow")
  Binds runtime-application.yaml into typed Java objects:
    WorkflowProperties
      └── id, description, entityIdField, deadLetterAfterRetries
      └── List<StateDefinition> states
      └── List<TopicMapping> topics
            └── topicName, mapsToState, preTransitionScript (nullable)
      └── GroovyConfig groovyScripts
            └── scriptsMountPath

KafkaInfraProperties.java
  @ConfigurationProperties(prefix = "kafka")
  Binds values.yaml kafka section:
    bootstrapServers, consumerGroupPrefix,
    securityProtocol, saslMechanism

── 3. DOMAIN ───────────────────────────────────────────────────
WorkflowEntity.java  (@Entity, table: workflow_state)
  Fields:
    entityId (PK), workflowId, currentState, previousState,
    stateChangedAt, receivedAt, sentToPowerAppsAt,
    recertifiedAt, sentToHmsAt, completedAt,
    errorMessage, retryCount,
    metadata (JSONB column — extra fields from payload),
    createdAt, updatedAt

WorkflowAuditLog.java  (@Entity, table: workflow_audit_log)
  Fields:
    id (PK, auto), entityId, workflowId,
    fromState, toState, topicName,
    transitionAt, triggeredBy (topic name),
    scriptExecuted (boolean), scriptDecision,
    skipReason, payload (JSONB)

── 4. DYNAMIC KAFKA LISTENER REGISTRATION ──────────────────────
WorkflowKafkaListenerRegistrar.java
  Implements ApplicationListener<ApplicationReadyEvent>
  On startup:
    - Reads all TopicMapping entries from WorkflowProperties
    - For each topic, creates a ConcurrentMessageListenerContainer
      with consumer group ID: {consumerGroupPrefix}-{workflowId}-{topicName}
    - Registers and starts each container
    - Delegates each message to WorkflowMessageHandler

WorkflowMessageHandler.java
  For each received Kafka message:
    1. Parse JSON payload to Map<String, Object>
    2. Extract entityId using configured entityIdField
    3. If preTransitionScript configured → execute via GroovyHookExecutor
    4. Based on StateDecision:
       - PROCEED  → call WorkflowStateService.transition(entityId, mapsToState)
       - OVERRIDE → call WorkflowStateService.transition(entityId, overrideState)
       - SKIP     → log skip reason, no DB update
    5. Always write to WorkflowAuditLog

── 5. GROOVY HOOK EXECUTOR ─────────────────────────────────────
StateDecision.java
  Public class with static factory methods:
    proceed(), skip(String reason), override(String newState)
  Fields: Action enum {PROCEED, SKIP, OVERRIDE},
          overrideState, skipReason

GroovyHookExecutor.java
  - Uses javax.script.ScriptEngineManager (JSR-223)
  - Loads Groovy engine once at startup
  - loadScript(String scriptFileName): reads from scriptsMountPath,
    caches compiled script
  - execute(String scriptFile, Map payload, String currentState,
            String entityId): returns StateDecision
  - Injects payload, currentState, entityId, StateDecision into bindings
  - Handles script errors gracefully → returns StateDecision.skip(errorMsg)
    and logs the exception (never crashes the consumer)

── 6. SERVICE LAYER ────────────────────────────────────────────
WorkflowStateService.java
  transition(String entityId, String newState, TransitionContext ctx):
    - Load existing WorkflowEntity or create new
    - Idempotency check: if currentState == newState → log and return
    - Update currentState, previousState, stateChangedAt
    - Set specific timestamp field based on newState
      (e.g. sentToPowerAppsAt when newState=SENT_TO_POWERAPP)
    - Increment retryCount if transitioning to FAILED
    - Auto-transition to DEAD_LETTERED if retryCount >= deadLetterAfterRetries
    - Persist entity
    - Write audit log entry

── 7. REPOSITORY ───────────────────────────────────────────────
WorkflowRepository.java  (Spring Data JPA)
Custom query methods:
  findByWorkflowIdAndCurrentState(workflowId, state)
  findByWorkflowIdAndCurrentStateIn(workflowId, List<states>)
  findByEntityIdAndWorkflowId(entityId, workflowId)
  countByWorkflowIdGroupByCurrentState(workflowId)

WorkflowAuditLogRepository.java
  findByEntityIdOrderByTransitionAtAsc(entityId)

── 8. REST API ─────────────────────────────────────────────────
WorkflowDashboardController.java
  All endpoints return standard ApiResponse<T> wrapper with
  status, message, data, timestamp.

  GET  /api/workflow/info
       → Returns workflowId, description, total registered topics,
         active listener count, uptime

  GET  /api/workflow/summary
       → Count of entities grouped by currentState

  GET  /api/workflow/active
       → All entities NOT in terminal states
         (COMPLETED, FAILED, DEAD_LETTERED)

  GET  /api/workflow/pending-recertification
       → Entities in PENDING_RECERTIFICATION or SENT_TO_POWERAPP

  GET  /api/workflow/recertified
       → Entities in RECERTIFIED or RECERT_REJECTED

  GET  /api/workflow/sent-to-hms
       → Entities in SENT_TO_HMS or COMPLETED

  GET  /api/workflow/failed
       → Entities in FAILED or DEAD_LETTERED

  GET  /api/workflow/entity/{entityId}
       → Full entity detail + complete audit trail
         (all state transitions in order)

  GET  /api/workflow/health
       → Liveness check: lists workflowId, registered topics,
         Kafka listener status (RUNNING/STOPPED) per topic

── 9. DATABASE ─────────────────────────────────────────────────
schema.sql:
  CREATE SCHEMA IF NOT EXISTS ${schema};

  workflow_state table:
    entity_id, workflow_id, current_state, previous_state,
    state_changed_at, received_at, sent_to_power_apps_at,
    recertified_at, sent_to_hms_at, completed_at,
    error_message, retry_count,
    metadata JSONB,
    created_at, updated_at
    PRIMARY KEY (entity_id)
    INDEX on (workflow_id, current_state)
    INDEX on (workflow_id, state_changed_at)

  workflow_audit_log table:
    id BIGSERIAL PK,
    entity_id, workflow_id,
    from_state, to_state, topic_name,
    transition_at, script_executed BOOLEAN,
    script_decision VARCHAR, skip_reason,
    payload JSONB
    INDEX on (entity_id)
    INDEX on (workflow_id, transition_at)

── 10. APPLICATION YAML (baked into image) ─────────────────────
src/main/resources/application.yaml
  Server config (port 8080)
  Spring datasource from env vars:
    url: jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}
         ?currentSchema=${DB_SCHEMA}
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}
  JPA: ddl-auto=validate, dialect=PostgreSQL
  Kafka: bootstrap-servers from ${KAFKA_BOOTSTRAP_SERVERS}
  Logging config
  Management endpoints for health/info

  NOTE: workflow.* properties are NOT here.
  They come from runtime-application.yaml via
  SPRING_CONFIG_ADDITIONAL_LOCATION at runtime.

── 11. DOCKERFILE ──────────────────────────────────────────────
Multi-stage Dockerfile:
  Stage 1 (builder): maven:3.9-eclipse-temurin-21
    - Copy pom.xml and src
    - mvn package -DskipTests
  Stage 2 (runtime): eclipse-temurin:21-jre-jammy
    - Copy JAR from builder
    - Create non-root user (appuser)
    - EXPOSE 8080
    - ENTRYPOINT java -jar app.jar

── 12. HELM CHART ──────────────────────────────────────────────
Full Helm chart structure:

  helm/
    Chart.yaml
    values.yaml                        ← infra config (see above)
    runtime-application.yaml           ← workflow business config (see above)
    scripts/
      validator-hook.groovy            ← example hook scripts
      recert-hook.groovy
    templates/
      deployment.yaml
      service.yaml
      configmap-runtime.yaml           ← mounts runtime-application.yaml
      configmap-scripts.yaml           ← mounts all files from scripts/
      secret.yaml                      ← dbPassword, kafkaPassword
      hpa.yaml
      ingress.yaml
      serviceaccount.yaml

  deployment.yaml must:
    - Set env vars from values.yaml:
        KAFKA_BOOTSTRAP_SERVERS, DB_HOST, DB_PORT, DB_NAME,
        DB_SCHEMA, DB_USERNAME, DB_PASSWORD (from secret),
        KAFKA_PASSWORD (from secret)
    - Set:
        SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/runtime-application.yaml
    - Mount configmap-runtime → /app/config/
    - Mount configmap-scripts → /app/scripts/
    - Configure livenessProbe and readinessProbe on
        GET /api/workflow/health
    - Set resource requests and limits

  configmap-runtime.yaml:
    data:
      runtime-application.yaml: |
        {{ .Files.Get "runtime-application.yaml" | indent 8 }}

  configmap-scripts.yaml:
    data:
      {{ range $path, $content := .Files.Glob "scripts/*.groovy" }}
      {{ base $path }}: |
        {{ $content | indent 8 }}
      {{ end }}

── 13. README.md ───────────────────────────────────────────────
Include:
  a) Architecture overview diagram (ASCII)
  b) How to deploy a new workflow (step by step):
       1. Copy helm/ folder
       2. Edit values.yaml (infra settings)
       3. Edit runtime-application.yaml (workflow states + topics)
       4. Add Groovy scripts to scripts/ if needed
       5. helm install {release-name} ./helm
  c) How to add a new Kafka topic to existing workflow:
       Edit runtime-application.yaml → helm upgrade
  d) How to update a Groovy script:
       Edit scripts/*.groovy → helm upgrade (rolling restart)
  e) Environment variable reference table
  f) REST API reference table
  g) Groovy StateDecision API reference

═══════════════════════════════════════════════════════════════
KEY CONSTRAINTS — enforce all of these
═══════════════════════════════════════════════════════════════

- Spring Boot app.yaml has NO workflow config — only infra/server config
- All workflow config lives exclusively in runtime-application.yaml
- No Kafka producing anywhere in the codebase
- Groovy script errors must NEVER crash the Kafka consumer —
  always catch, log, and return StateDecision.skip(errorMessage)
- Consumer group ID pattern must be:
    {consumerGroupPrefix}-{workflowId}-{topicName}
- Idempotent: same state received twice = no-op, no error
- One WorkflowEntity per entityId (upsert pattern)
- Audit log records EVERY transition attempt including skips
- All sensitive config (passwords) via Kubernetes Secret only —
  never in ConfigMap or values.yaml plaintext
- Docker image runs as non-root user
- Health endpoint must report Kafka listener status per topic

═══════════════════════════════════════════════════════════════
IMPLEMENTATION ORDER
═══════════════════════════════════════════════════════════════

Implement in this order (so each part compiles before the next):

  1. pom.xml with all dependencies
  2. WorkflowProperties + KafkaInfraProperties
  3. Domain entities and StateDecision
  4. application.yaml (image-baked config only)
  5. GroovyHookExecutor
  6. WorkflowStateService + repositories
  7. WorkflowKafkaListenerRegistrar + WorkflowMessageHandler
  8. WorkflowDashboardController + DTOs
  9. schema.sql
  10. Dockerfile
  11. Helm chart (all templates)
  12. Example runtime-application.yaml + example Groovy scripts
  13. README.md
