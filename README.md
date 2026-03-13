# kafka-datadog-client-gcp

Standalone **Datadog Agent** deployment that monitors **Google Cloud Managed Service for Apache Kafka** using a custom Python check with `SASL/OAUTHBEARER` + Workload Identity.

Runs as a **single-replica Deployment** (not a DaemonSet). No JMX. All metrics come from the Kafka Admin API and Consumer API via `confluent-kafka`.

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│  GKE Cluster                                              │
│                                                           │
│  Deployment: datadog-agent-kafka  (namespace: datadog)    │
│  ┌─────────────────────────────┐                          │
│  │ datadog-kafka-agent:YOUR_TAG │  1 replica               │
│  │  datadog/agent:7            │                          │
│  │  + google-auth   (pip)      │  → kafka_consumer_gcp    │
│  │  + confluent-kafka (pip)    │  → GCP OAuth token       │
│  └────────────┬────────────────┘                          │
│               │ Workload Identity (ADC)                   │
│               ▼                                           │
│  Google Cloud Managed Kafka  (SASL/OAUTHBEARER, port 9092)│
└───────────────────────────────────────────────────────────┘
```

## Project structure

```
datadog-kafka-agent/
├── Dockerfile                          # FROM datadog/agent:7 + pip install + COPY check
├── checks.d/
│   └── kafka_consumer_gcp.py           # Custom check (baked into image)
├── k8s/
│   └── datadog-agent-kafka.yaml        # Deployment + ConfigMap (kubectl apply)
└── README.md
```

**Key design decisions:**
- The Dockerfile installs `google-auth`, `confluent-kafka`, `urllib3` via the agent's embedded pip and bakes `kafka_consumer_gcp.py` into the image.
- Check *configuration* (bootstrap, consumer groups, flags) lives in a ConfigMap mounted over `conf.d/`.
- `/etc/datadog-agent/datadog.yaml` is overridden (via the symlink target `datadog-kubernetes.yaml`) to disable all built-in Kubernetes checks — only `kafka_consumer_gcp` runs.
- `conf.d/` is replaced entirely by the ConfigMap so zero built-in autoconf templates load.

---

## Metrics emitted

All metrics use **GAUGE** type. Mandatory tags on every metric: `cluster:{name}`, `env:{env}`, `region:{gcp_region}`, `team:kafka-admins`.  
Additional tags are listed per group.

### Cluster-level  (`kafka_gcp.cluster.*`)

| Metric | Description |
|---|---|
| `kafka_gcp.cluster.brokers` | Number of brokers in the cluster |
| `kafka_gcp.cluster.topics` | Number of topics (excluding internal unless opted in) |
| `kafka_gcp.cluster.partitions` | Total partition count across all topics |
| `kafka_gcp.cluster.under_replicated_partitions` | Partitions where ISR < replication factor |
| `kafka_gcp.cluster.offline_partitions` | Partitions without a leader |
| `kafka_gcp.cluster.controller_id` | Broker ID of the active controller |

### Broker-level  (`kafka_gcp.broker.*`)  — tag: `broker_id`

| Metric | Description |
|---|---|
| `kafka_gcp.broker.leader_partitions` | Partitions for which this broker is leader |
| `kafka_gcp.broker.total_partitions` | Total replicas (leader + follower) on this broker |
| `kafka_gcp.broker.topics_count` | Distinct topics this broker holds at least one replica of |

### Topic-level  (`kafka_gcp.topic.*`)  — tag: `topic`

| Metric | Description |
|---|---|
| `kafka_gcp.topic.partitions` | Number of partitions |
| `kafka_gcp.topic.replication_factor` | Replication factor |
| `kafka_gcp.topic.under_replicated_partitions` | Under-replicated partitions |
| `kafka_gcp.topic.offline_partitions` | Partitions without a leader |
| `kafka_gcp.topic.records` | Record count = Σ(high − low watermark) per partition |
| `kafka_gcp.topic.log_size_bytes` | Disk usage (requires `monitor_log_dir_sizes: true`; may be unsupported) |

### Consumer groups  (`kafka_gcp.consumer_group*`)

| Metric | Additional tags | Description |
|---|---|---|
| `kafka_gcp.consumer_groups.total` | — | Total number of consumer groups |
| `kafka_gcp.consumer_groups.by_state` | `state` | Count of groups per state |
| `kafka_gcp.consumer_group.info` | `consumer_group`, `state` | 1 per group (use for grouping/filtering by state) |
| `kafka_gcp.consumer_group.state` | `consumer_group` | Numeric state: 0=unknown 1=dead 2=empty 3=preparingrebalance 4=completingrebalance 5=stable |
| `kafka_gcp.consumer_group.members` | `consumer_group` | Member count (requires `collect_consumer_group_details: true`) |
| `kafka_gcp.consumer_group.assigned_partitions` | `consumer_group` | Assigned partition count (requires `collect_consumer_group_details: true`) |

### Consumer lag  — tags: `consumer_group`, `topic`, `partition`

| Metric | Description |
|---|---|
| `kafka_gcp.consumer_lag` | high_watermark − committed_offset |
| `kafka_gcp.consumer_offset` | Committed offset |
| `kafka_gcp.broker_offset` | High-watermark offset |

### Service check

`kafka_consumer_gcp.can_connect` — `OK` / `CRITICAL`

---

## Prerequisites

1. **GKE** cluster with [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity) enabled.
2. A **GCP Service Account** with roles:
   - `roles/managedkafka.client`
   - `roles/iam.serviceAccountTokenCreator`
3. **Workload Identity binding** between the GKE KSA and the GCP SA (see below).
4. **Datadog API key secret** `datadog-agent` in the `datadog` namespace.

---

## Step 1 — Build & push the image

```bash
cd datadog-kafka-agent

# Authenticate Docker to Artifact Registry (once)
gcloud auth configure-docker YOUR_REGION-docker.pkg.dev

# Build for linux/amd64 (GKE nodes)
IMAGE=YOUR_REGISTRY/datadog-kafka-agent

docker buildx build --platform linux/amd64 -t ${IMAGE}:YOUR_TAG .

# Push
docker push ${IMAGE}:YOUR_TAG
```

> **To release a new version**, bump the tag (e.g. `0.0.2`) in both the `docker buildx` / `docker push` commands above and in the `image:` field of `k8s/datadog-agent-kafka.yaml`, then re-apply.

---

## Step 2 — Bind Workload Identity

```bash
GCP_SA="kafka-sa@YOUR_PROJECT.iam.gserviceaccount.com"
GCP_PROJECT="YOUR_PROJECT"
K8S_NS="datadog"
K8S_SA="datadog-agent-kafka"

gcloud iam service-accounts add-iam-policy-binding ${GCP_SA} \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:${GCP_PROJECT}.svc.id.goog[${K8S_NS}/${K8S_SA}]"
```

---

## Step 3 — Deploy

```bash
kubectl apply -f k8s/datadog-agent-kafka.yaml
kubectl -n datadog rollout status deployment/datadog-agent-kafka
```

This creates in namespace `datadog`:
- **ServiceAccount** `datadog-agent-kafka` (annotated for Workload Identity)
- **ConfigMap** `datadog-agent-kafka-confd` (agent config + check config)
- **Deployment** `datadog-agent-kafka` (1 replica)

---

## Step 4 — Verify

```bash
# Pod running?
kubectl -n datadog get pods -l app=datadog-agent-kafka

# Run check once with full output
kubectl -n datadog exec -it deploy/datadog-agent-kafka -- agent check kafka_consumer_gcp

# Show metric names from a live run
kubectl -n datadog exec deploy/datadog-agent-kafka -- agent check kafka_consumer_gcp 2>&1 \
  | grep '"metric"' | sed 's/.*"metric": "\([^"]*\)".*/\1/' | sort -u

# Agent status summary
kubectl -n datadog exec -it deploy/datadog-agent-kafka -- agent status \
  | grep -A 15 kafka_consumer_gcp

# Tail logs
kubectl -n datadog logs -f deploy/datadog-agent-kafka
```

Then search for `kafka_gcp.*` in **Datadog → Metrics → Explorer**.

---

## Configuration reference

Edit the `kafka_consumer_gcp.yaml` key in the ConfigMap section of `k8s/datadog-agent-kafka.yaml`:

```yaml
instances:
  - kafka_connect_str:
      - "bootstrap.CLUSTER.REGION.managedkafka.PROJECT.cloud.goog:9092"

    security_protocol: "SASL_SSL"
    sasl_mechanism:    "OAUTHBEARER"

    # Optional — auto-parsed from bootstrap URL when omitted
    # cluster_name: "my-kafka-cluster"
    # gcp_region:   "europe-west1"

    # Consumer groups to track lag for (glob/wildcard supported)
    consumer_groups:
      "*":        # discover all groups
        "*": []   # all topics, all partitions

    # ── Feature flags ──────────────────────────────────────────────────────
    monitor_cluster_metrics:        true   # cluster / broker / topic metadata
    monitor_consumer_group_states:  true   # consumer group state metrics
    monitor_record_counts:          true   # topic.records (high − low watermark)
    monitor_log_dir_sizes:          false  # topic.log_size_bytes (may be unsupported by GCP)
    collect_consumer_group_details: false  # member count via describe_consumer_groups
    include_internal_topics:        false  # include __consumer_offsets etc.
    record_count_timeout:           5      # seconds per watermark call

    tags:
      - env:YOUR_ENV
      - cloud:gcp
```

### Wildcard / glob support for consumer groups

```yaml
consumer_groups:
  "*":            # all groups, all topics
    "*": []

  "my-app-*":     # groups matching prefix
    "events-*": []
    "orders-*": [0, 1, 2]   # specific partitions

  "exact-group":
    "my-topic": []
```

---

## Debugging

```bash
# Enable debug logging temporarily
kubectl -n datadog set env deploy/datadog-agent-kafka DD_LOG_LEVEL=debug

# Run check with debug output
kubectl -n datadog exec -it deploy/datadog-agent-kafka -- \
  agent check kafka_consumer_gcp --log-level debug

# Restore normal log level
kubectl -n datadog set env deploy/datadog-agent-kafka DD_LOG_LEVEL=info
```

---

## How it works

### Why a custom check?

The Datadog built-in `kafka_consumer` check supports `sasl_mechanism: OAUTHBEARER` but its `sasl_oauth_token_provider` config only handles a generic OAuth2 URL flow — it cannot produce the custom `GOOG_OAUTH2_TOKEN` JWT that Google Managed Kafka requires.

### GcpOAuthTokenProvider (Python ≡ Java GcpLoginCallbackHandler)

| Java (standard Kafka client) | Python (this check) |
|---|---|
| `GcpLoginCallbackHandler` class | `GcpOAuthTokenProvider` class |
| JVM ADC via `ApplicationDefaultCredentials` | `google.auth.default()` |
| `sasl.login.callback.handler.class=…` | `oauth_cb` callback in confluent-kafka config |
| JWT with `alg=GOOG_OAUTH2_TOKEN` | Built manually: `header.payload.access_token` (base64url) |

### librdkafka quirks

Two non-obvious issues (already fixed in the check):

1. **`AdminClient` never polls** — librdkafka's `AdminClient` doesn't call `rd_kafka_poll()` internally, so the `oauth_cb` token callback is never triggered. Fix: call `admin.poll(5000)` once after construction.
2. **Bundled OpenSSL CA path** — librdkafka ships its own OpenSSL binary with a different default CA bundle path than the system. Fix: `ssl.ca.location: /etc/ssl/certs/cacert.pem`.

### Agent config override

The Datadog Agent image uses `/etc/datadog-agent/datadog.yaml` as a **symlink** → `datadog-kubernetes.yaml`. Kubernetes `subPath` mounts can't override symlinks, so we mount our ConfigMap directly on the symlink's **real target** (`datadog-kubernetes.yaml`). This disables all Kubernetes auto-discovery checks. The `conf.d/` directory is replaced entirely by the ConfigMap so only `kafka_consumer_gcp` runs.

