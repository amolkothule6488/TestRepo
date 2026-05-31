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
      - All sensitive values injected as Kubernetes Secrets only

   b) runtime-application.yaml — Application / workflow business config
      - Workflow ID, description, entityIdField
      - Complete state definitions with terminal flags
      - Kafka topic → state mappings
      - Optional Groovy pre-transition script reference per topic
      - Scripts mount path
      - Dead letter retry threshold
      Mounted into the pod as a ConfigMap at /app/config/
      Loaded by Spring Boot via:
        SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/runtime-application.yaml

   The image-baked application.yaml contains ONLY:
   server config, datasource placeholders, Kafka placeholders,
   JPA config, logging, and management endpoints.
   It contains ZERO workflow business config.

3. GROOVY PRE-TRANSITION HOOKS
   Each Kafka topic can optionally reference a Groovy script.
   Scripts are stored in helm/scripts/, rendered into a ConfigMap,
   and mounted at /app/scripts/ inside the pod.
   The engine executes the script BEFORE updating state.
   Scripts receive: payload (Map), currentState (String),
   entityId (String), StateDecision (class reference).
   Scripts must return a StateDecision.
   Script errors must NEVER crash the consumer —
   always catch, log, return StateDecision.skip(errorMessage).

4. PASSIVE KAFKA CONSUMER
   This service only CONSUMES — never produces.
   It uses a unique consumer group ID per topic:
     {consumerGroupPrefix}-{workflowId}-{topicName}
   Existing pipeline consumers are NEVER affected.

5. IDEMPOTENT STATE TRANSITIONS
   Receiving the same Kafka message twice must never cause
   errors or duplicate state updates.

6. GENERIC REST API
   Zero business-specific endpoint names in the base image.
   All state names are runtime values from runtime-application.yaml.
   Clients query by passing state names as request parameters.
   Terminal vs non-terminal distinction driven by
   runtime-application.yaml state definitions, not Java code.

═══════════════════════════════════════════════════════════════
CONFIGURATION FILE SPECIFICATIONS
═══════════════════════════════════════════════════════════════

── values.yaml ─────────────────────────────────────────────────

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

── runtime-application.yaml ────────────────────────────────────

workflow:
  id: "SPRINGBROOK_BOOK_FLOW"
  description: "Springbrook end-to-end book processing"
  entityIdField: "bookId"
  deadLetterAfterRetries: 3

  states:
    - name: "RECEIVED"
      terminal: false
    - name: "VALIDATED"
      terminal: false
    - name: "VALIDATION_FAILED"
      terminal: false
    - name: "TRANSFORMED"
      terminal: false
    - name: "PERSISTED"
      terminal: false
    - name: "SENT_TO_POWERAPP"
      terminal: false
    - name: "PENDING_RECERTIFICATION"
      terminal: false
    - name: "RECERTIFIED"
      terminal: false
    - name: "RECERT_REJECTED"
      terminal: true
    - name: "SENT_TO_HMS"
      terminal: false
    - name: "COMPLETED"
      terminal: true
    - name: "FAILED"
      terminal: true
    - name: "DEAD_LETTERED"
      terminal: true

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
    → do not update state, log reason, write audit entry

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

  // transformer-hook.groovy
  if (payload.bookType == 'DRAFT') {
      return StateDecision.skip('Draft books not tracked')
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
- Groovy JSR-223 (groovy-jsr223 dependency)
- Lombok
- Maven
- Docker (multi-stage)
- Helm 3 (GCP deployment)

═══════════════════════════════════════════════════════════════
DELIVERABLES
═══════════════════════════════════════════════════════════════

Implement ALL of the following in this exact order:

── 1. PROJECT STRUCTURE ────────────────────────────────────────

Complete Maven project layout:

  com.springbrook.tracker
    ├── config
    │     KafkaConsumerConfig.java
    │     GroovyEngineConfig.java
    ├── domain
    │     WorkflowEntity.java
    │     WorkflowAuditLog.java
    │     StateDecision.java
    ├── properties
    │     WorkflowProperties.java      (@ConfigurationProperties)
    │     KafkaInfraProperties.java    (@ConfigurationProperties)
    ├── kafka
    │     WorkflowKafkaListenerRegistrar.java
    │     WorkflowMessageHandler.java
    ├── groovy
    │     GroovyHookExecutor.java
    ├── service
    │     WorkflowStateService.java
    ├── repository
    │     WorkflowRepository.java
    │     WorkflowAuditLogRepository.java
    └── api
          WorkflowDashboardController.java
          dto/
            ApiResponse.java
            WorkflowEntityDto.java
            WorkflowSummaryDto.java
            WorkflowInfoDto.java
            AuditLogDto.java
            StateDefinitionDto.java
            PagedResponse.java

── 2. pom.xml ──────────────────────────────────────────────────

Include dependencies:
  spring-boot-starter-web
  spring-boot-starter-data-jpa
  spring-kafka
  postgresql driver
  groovy-jsr223
  jackson-databind
  lombok
  spring-boot-configuration-processor
  spring-boot-starter-actuator
  spring-boot-starter-validation

── 3. PROPERTIES BINDING ───────────────────────────────────────

WorkflowProperties.java
  @ConfigurationProperties(prefix = "workflow")
  Typed binding for runtime-application.yaml:

    String id
    String description
    String entityIdField
    int deadLetterAfterRetries
    List<StateDefinition> states
      └── String name
          boolean terminal
    List<TopicMapping> topics
      └── String topicName
          String mapsToState
          String preTransitionScript  (nullable)
    GroovyConfig groovyScripts
      └── String scriptsMountPath

  Helper methods:
    isTerminal(String stateName) → boolean
    getAllTerminalStates()        → List<String>
    getAllActiveStates()          → List<String>
    isValidState(String name)    → boolean

KafkaInfraProperties.java
  @ConfigurationProperties(prefix = "kafka")
    String bootstrapServers
    String consumerGroupPrefix
    String securityProtocol
    String saslMechanism

── 4. DOMAIN ───────────────────────────────────────────────────

WorkflowEntity.java  (@Entity, table: workflow_state)
  @Id  String entityId
  String workflowId
  String currentState
  String previousState
  LocalDateTime stateChangedAt
  LocalDateTime receivedAt
  LocalDateTime sentToPowerAppsAt
  LocalDateTime recertifiedAt
  LocalDateTime sentToHmsAt
  LocalDateTime completedAt
  String errorMessage
  int retryCount
  @Column(columnDefinition="jsonb") String metadata
  LocalDateTime createdAt
  LocalDateTime updatedAt

WorkflowAuditLog.java  (@Entity, table: workflow_audit_log)
  @Id @GeneratedValue  Long id
  String entityId
  String workflowId
  String fromState
  String toState
  String topicName
  LocalDateTime transitionAt
  boolean scriptExecuted
  String scriptDecision   (PROCEED / SKIP / OVERRIDE)
  String skipReason
  @Column(columnDefinition="jsonb") String payload

StateDecision.java
  public enum Action { PROCEED, SKIP, OVERRIDE }
  private final Action action
  private final String overrideState
  private final String skipReason
  public static StateDecision proceed()
  public static StateDecision skip(String reason)
  public static StateDecision override(String newState)
  getters for all fields

── 5. CONFIGURATION CLASSES ────────────────────────────────────

KafkaConsumerConfig.java
  ConsumerFactory bean using KafkaInfraProperties
  StringDeserializer for key and value
  SASL config wired from properties
  Separate ConcurrentKafkaListenerContainerFactory bean

GroovyEngineConfig.java
  ScriptEngine bean — initialize Groovy JSR-223 engine once
  at application startup

── 6. GROOVY HOOK EXECUTOR ─────────────────────────────────────

GroovyHookExecutor.java
  Inject ScriptEngine and WorkflowProperties
  Cache compiled scripts at startup:
    Map<String, CompiledScript> scriptCache
  On startup (@PostConstruct):
    For each topic with preTransitionScript configured:
      Load file from scriptsMountPath
      Compile and cache
  execute(String scriptFile, Map payload,
          String currentState, String entityId):
    → Retrieve compiled script from cache
    → Create Bindings with payload, currentState, entityId, StateDecision
    → Execute script
    → Cast result to StateDecision
    → On ANY exception: log error, return StateDecision.skip(errorMessage)
    → Never throw — consumer must never crash from script errors

── 7. KAFKA LISTENER REGISTRATION ──────────────────────────────

WorkflowKafkaListenerRegistrar.java
  Implements ApplicationListener<ApplicationReadyEvent>
  Inject WorkflowProperties, KafkaInfraProperties,
         ConcurrentKafkaListenerContainerFactory,
         WorkflowMessageHandler
  On ApplicationReadyEvent:
    For each TopicMapping in workflow.topics:
      consumerGroupId =
        {consumerGroupPrefix}-{workflowId}-{topicName}
      Create ContainerProperties for topicName
      Create ConcurrentMessageListenerContainer
      Set MessageListener → WorkflowMessageHandler.handle(record, topicMapping)
      Start container
      Store in Map<String, ConcurrentMessageListenerContainer>
        keyed by topicName (for health reporting)
  Expose getListenerStatus(topicName) → RUNNING | STOPPED

WorkflowMessageHandler.java
  handle(ConsumerRecord<String,String> record, TopicMapping mapping):
    1. Parse record.value() as Map<String, Object>
    2. Extract entityId using workflow.entityIdField
    3. If entityId blank → log warning, return (skip unidentifiable messages)
    4. If mapping.preTransitionScript not null:
         decision = groovyHookExecutor.execute(script, payload,
                      currentState, entityId)
       Else:
         decision = StateDecision.proceed()
    5. Switch on decision.action:
         PROCEED  → stateService.transition(entityId, mapping.mapsToState, ctx)
         OVERRIDE → stateService.transition(entityId, decision.overrideState, ctx)
         SKIP     → log skip, write audit log entry only (no state change)
    6. Always write WorkflowAuditLog entry regardless of decision

── 8. SERVICE LAYER ────────────────────────────────────────────

WorkflowStateService.java
  transition(String entityId, String newState, TransitionContext ctx):
    1. Load WorkflowEntity by entityId or create new
    2. Idempotency: if entity.currentState == newState → log, return
    3. Validate newState exists in WorkflowProperties.states → log warn if not
    4. Set previousState = currentState
    5. Set currentState = newState
    6. Set stateChangedAt = now()
    7. Set specific timestamp based on newState:
         RECEIVED          → receivedAt
         SENT_TO_POWERAPP  → sentToPowerAppsAt
         RECERTIFIED       → recertifiedAt
         SENT_TO_HMS       → sentToHmsAt
         COMPLETED         → completedAt
    8. If newState == FAILED → increment retryCount
    9. If retryCount >= deadLetterAfterRetries → override to DEAD_LETTERED
    10. Persist entity (save)
    11. Write WorkflowAuditLog entry

  getCurrentState(String entityId) → Optional<String>

TransitionContext:
  String topicName
  String scriptExecuted
  String scriptDecision
  String skipReason
  Map<String, Object> rawPayload

── 9. REPOSITORY ───────────────────────────────────────────────

WorkflowRepository.java (JpaRepository<WorkflowEntity, String>)
  Page<WorkflowEntity> findByCurrentState(String state, Pageable p)
  Page<WorkflowEntity> findByCurrentStateIn(List<String> states, Pageable p)
  Optional<WorkflowEntity> findByEntityId(String entityId)
  List<Object[]> countGroupByCurrentState()  (@Query)

WorkflowAuditLogRepository.java
  List<WorkflowAuditLog> findByEntityIdOrderByTransitionAtAsc(String id)

── 10. REST API ─────────────────────────────────────────────────

WorkflowDashboardController.java
  Base path: /api/workflow
  All endpoints fully generic — zero business state names hardcoded.
  All state names are runtime values from WorkflowProperties.
  All responses wrapped in ApiResponse<T>.

  ── Introspection ──────────────────────────────────────

  GET /api/workflow/info
    Returns WorkflowInfoDto:
      workflowId, description, entityIdField,
      totalTopics, registeredListeners, uptime,
      configuredStates: [{ name, terminal }]

  GET /api/workflow/health
    Returns per-topic listener health:
      [{ topicName, mapsToState, listenerStatus: RUNNING|STOPPED,
         scriptConfigured: true|false }]
    Used as liveness + readiness probe in Helm deployment.yaml

  GET /api/workflow/states
    Returns all configured states from WorkflowProperties:
      [{ name, terminal }]
    Allows clients to discover valid state names dynamically
    before constructing ?state= queries.

  ── Aggregate ──────────────────────────────────────────

  GET /api/workflow/summary
    Returns count per state:
      [{ state, count }]
    Sorted by count descending.

  ── Entity Queries — Generic, State-Driven ─────────────

  GET /api/workflow/entities
    Query params:
      state    → single or comma-separated list of state names
                 e.g. ?state=VALIDATED
                 e.g. ?state=FAILED,DEAD_LETTERED
      page     → default 0
      size     → default 20, max 100
      sortBy   → default stateChangedAt
      sortDir  → ASC | DESC, default DESC
    Validation:
      If any state value not in WorkflowProperties.states → 400 Bad Request
      with message: "Unknown states: [X, Y]. Valid states: [...]"
    Returns: PagedResponse<WorkflowEntityDto>

  GET /api/workflow/entities/active
    Returns entities where currentState is NOT terminal
    (reads terminal flag from WorkflowProperties — not hardcoded)
    Supports same pagination params.

  GET /api/workflow/entities/terminal
    Returns entities where currentState IS terminal.
    Supports same pagination params.

  ── Single Entity ───────────────────────────────────────

  GET /api/workflow/entity/{entityId}
    Returns WorkflowEntityDto:
      entityId, workflowId, currentState, previousState,
      stateChangedAt, receivedAt, sentToPowerAppsAt,
      recertifiedAt, sentToHmsAt, completedAt,
      retryCount, errorMessage, metadata

  GET /api/workflow/entity/{entityId}/history
    Returns ordered audit trail:
      [{ fromState, toState, topicName, transitionAt,
         scriptExecuted, scriptDecision, skipReason }]

  ── Error Handling ──────────────────────────────────────

  GlobalExceptionHandler (@RestControllerAdvice)
    400 → unknown state in ?state= param
    404 → entityId not found
    500 → unexpected error with safe generic message
    All wrapped in ApiResponse<Void> with errorMessage field.

  ── Response DTOs ───────────────────────────────────────

  ApiResponse<T>:
    String status  (SUCCESS | ERROR)
    String workflowId
    Instant timestamp
    T data
    String errorMessage  (nullable)

  PagedResponse<T> extends ApiResponse<List<T>>:
    int page, int size, long totalElements, int totalPages

  WorkflowEntityDto:
    entityId, workflowId, currentState, previousState,
    stateChangedAt, receivedAt, sentToPowerAppsAt,
    recertifiedAt, sentToHmsAt, completedAt,
    retryCount, errorMessage, metadata (Map)

  WorkflowInfoDto:
    workflowId, description, entityIdField,
    totalTopics, configuredStates (List<StateDefinitionDto>), uptime

  StateDefinitionDto:
    String name, boolean terminal

  AuditLogDto:
    entityId, fromState, toState, topicName,
    transitionAt, sc
