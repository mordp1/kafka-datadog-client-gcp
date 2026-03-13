"""
kafka_consumer_gcp  –  Datadog Agent custom check

Connects to Google Cloud Managed Service for Apache Kafka using
SASL/OAUTHBEARER + Application Default Credentials (Workload Identity)
and reports consumer-group lag and comprehensive cluster/broker/topic metrics.

Why a custom check?
───────────────────
The Datadog built-in ``kafka_consumer`` check supports ``sasl_mechanism: OAUTHBEARER``
but its ``sasl_oauth_token_provider`` only works with a generic URL-based OAuth2
flow (url + client_id + client_secret).

Google Cloud Managed Kafka requires a **non-standard** JWT with
``alg=GOOG_OAUTH2_TOKEN`` where the signature is the raw Google access token.
In Java this is handled by ``GcpLoginCallbackHandler`` — this check is the
**Python equivalent** for the Datadog Agent.

Configuration (same style as built-in kafka_consumer check):

    instances:
      - kafka_connect_str:
          - "bootstrap.CLUSTER.REGION.managedkafka.PROJECT.cloud.goog:9092"
        security_protocol: "SASL_SSL"
        sasl_mechanism: "OAUTHBEARER"

        # Optional — auto-parsed from bootstrap URL when omitted
        # cluster_name: "my-kafka-cluster"
        # gcp_region:   "europe-west1"

        # Consumer groups to track for lag (wildcard/glob supported)
        consumer_groups:
          "*":             # "*" = auto-discover ALL consumer groups
            "*": []        # "*" = all topics, [] = all partitions

        # Feature flags (all default-true except the expensive ones)
        monitor_cluster_metrics:       true   # cluster / broker / topic metadata
        monitor_consumer_group_states: true   # group state metrics + optional detail
        monitor_record_counts:         true   # topic record count (high - low watermark)
        monitor_log_dir_sizes:         false  # topic byte size via describe_log_dirs
        collect_consumer_group_details: false # describe_consumer_groups for member count
        include_internal_topics:       false  # include __consumer_offsets etc.
        record_count_timeout:          5      # seconds per get_watermark_offsets call

        tags:
          - env:YOUR_ENV
          - cloud:gcp

Metrics emitted  (prefix kafka_gcp.*)
──────────────────────────────────────
  CLUSTER (tags: cluster, env, region, team:kafka-admins)
    kafka_gcp.cluster.brokers
    kafka_gcp.cluster.topics
    kafka_gcp.cluster.partitions
    kafka_gcp.cluster.under_replicated_partitions
    kafka_gcp.cluster.offline_partitions
    kafka_gcp.cluster.controller_id

  BROKER (additional tag: broker_id)
    kafka_gcp.broker.leader_partitions
    kafka_gcp.broker.total_partitions
    kafka_gcp.broker.topics_count

  TOPIC (additional tag: topic)
    kafka_gcp.topic.partitions
    kafka_gcp.topic.replication_factor
    kafka_gcp.topic.under_replicated_partitions
    kafka_gcp.topic.offline_partitions
    kafka_gcp.topic.records              (requires monitor_record_counts)
    kafka_gcp.topic.log_size_bytes       (requires monitor_log_dir_sizes)

  CONSUMER GROUP (additional tags: consumer_group, state)
    kafka_gcp.consumer_groups.total
    kafka_gcp.consumer_groups.by_state          (tag: state)
    kafka_gcp.consumer_group.info               (always 1 — use state tag for grouping)
    kafka_gcp.consumer_group.state              (numeric: 0=unknown 1=dead 2=empty
                                                 3=preparingrebalance 4=completingrebalance
                                                 5=stable)
    kafka_gcp.consumer_group.members            (requires collect_consumer_group_details)
    kafka_gcp.consumer_group.assigned_partitions (requires collect_consumer_group_details)

  CONSUMER LAG (tags: consumer_group, topic, partition)
    kafka_gcp.consumer_lag
    kafka_gcp.consumer_offset
    kafka_gcp.broker_offset

  SERVICE CHECK
    kafka_consumer_gcp.can_connect
"""

import base64
import datetime
import fnmatch
import json
import logging
import re
import time
import traceback
from collections import defaultdict

import google.auth
from google.auth.transport.urllib3 import Request as GoogleAuthRequest
import urllib3

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from datadog_checks.base import AgentCheck

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

# Parses "bootstrap.<cluster>.<region>.managedkafka.<project>.cloud.goog[:port]"
_GCP_BOOTSTRAP_RE = re.compile(r"bootstrap\.([^.:]+)\.([^.:]+)\.managedkafka\.")

# Maps ConsumerGroupState name → numeric value for alerting
_CG_STATE_NUMERIC = {
    "unknown":               0,
    "dead":                  1,
    "empty":                 2,
    "preparingrebalance":    3,
    "completingrebalance":   4,
    "stable":                5,
}


# ─── Google OAuth Token Provider ────────────────────────────
# Python equivalent of com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler

def _b64(source: str) -> str:
    return base64.urlsafe_b64encode(source.encode("utf-8")).decode("utf-8").rstrip("=")


class GcpOAuthTokenProvider:
    """
    Python equivalent of the Java ``GcpLoginCallbackHandler``.

    Uses Application Default Credentials (ADC) obtained automatically
    via GKE Workload Identity to produce the GOOG_OAUTH2_TOKEN JWT
    that Google Managed Kafka expects for SASL/OAUTHBEARER.
    """

    HEADER = json.dumps({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"})

    def __init__(self):
        self.credentials, self.project = google.auth.default()
        self.http_client = urllib3.PoolManager()

    def _refresh(self):
        if not self.credentials.valid:
            self.credentials.refresh(GoogleAuthRequest(self.http_client))
        return self.credentials

    def _jwt_payload(self, creds):
        return json.dumps({
            "exp": creds.expiry.replace(tzinfo=datetime.timezone.utc).timestamp(),
            "iat": datetime.datetime.now(datetime.timezone.utc).timestamp(),
            "iss": "Google",
            "sub": creds.service_account_email,
        })

    def get_token(self, _oauth_config):
        """
        confluent-kafka ``oauth_cb`` callback.
        Returns (token_string, expiry_epoch_seconds).
        """
        try:
            log.info("oauth_cb called — refreshing GCP credentials…")
            creds = self._refresh()
            log.info(
                "GCP credentials refreshed: sa=%s, valid=%s, expiry=%s, token_prefix=%s",
                getattr(creds, 'service_account_email', 'N/A'),
                creds.valid,
                creds.expiry,
                (creds.token or '')[:20] + '…' if creds.token else 'NONE',
            )
            payload = self._jwt_payload(creds)
            token = ".".join([
                _b64(self.HEADER),
                _b64(payload),
                _b64(creds.token),
            ])
            expiry_utc = creds.expiry.replace(tzinfo=datetime.timezone.utc)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            lifetime = (expiry_utc - now_utc).total_seconds()
            log.info("JWT built successfully (length=%d, lifetime=%.0fs)", len(token), lifetime)
            return token, time.time() + lifetime
        except Exception as e:
            log.error("oauth_cb FAILED: %s\n%s", e, traceback.format_exc())
            raise


# ── Datadog Agent Check ────────────────────────────────────────────────────────

class KafkaConsumerGcpCheck(AgentCheck):
    """
    Collects Kafka metrics from Google Cloud Managed Kafka
    using Google OAuth (SASL/OAUTHBEARER).
    """

    _admin = None
    _consumer = None
    _token_provider = None

    # ── Tag helpers ──────────────────────────────────────────────────────────

    def _build_base_tags(self, instance):
        """
        Build the canonical tag list for this check instance.

        Always ensures: cluster:{name}, env:{env}, region:{region}, team:kafka-admins
        Merges with any instance-level ``tags``.

        cluster_name and gcp_region are read from:
          1. Explicit instance keys  cluster_name / gcp_region
          2. Auto-parsed from the GCP Managed Kafka bootstrap URL
        """
        bootstrap = instance.get("kafka_connect_str", [])
        bootstrap_str = ",".join(bootstrap) if isinstance(bootstrap, list) else str(bootstrap)

        parsed_cluster, parsed_region = None, None
        m = _GCP_BOOTSTRAP_RE.search(bootstrap_str)
        if m:
            parsed_cluster, parsed_region = m.group(1), m.group(2)

        cluster_name = instance.get("cluster_name") or parsed_cluster or "unknown"
        gcp_region   = instance.get("gcp_region")   or parsed_region  or "unknown"

        existing_tags = list(instance.get("tags", []))

        # Extract env from existing tags (env:prod → "prod")
        env_value = "unknown"
        for t in existing_tags:
            if str(t).startswith("env:"):
                env_value = t[4:]
                break

        mandatory = {
            f"cluster:{cluster_name}",
            f"env:{env_value}",
            f"region:{gcp_region}",
            "team:kafka-admins",
        }
        existing_set = set(existing_tags)
        return existing_tags + sorted(mandatory - existing_set)

    # ── Kafka client helpers ──────────────────────────────────────────────────

    def _build_kafka_config(self, instance):
        """
        Build confluent-kafka config from the check instance.
        Same keys as the built-in kafka_consumer check where possible.
        """
        bootstrap = instance.get("kafka_connect_str", [])
        if isinstance(bootstrap, list):
            bootstrap = ",".join(bootstrap)

        security_protocol = instance.get("security_protocol", "SASL_SSL")
        sasl_mechanism    = instance.get("sasl_mechanism",    "OAUTHBEARER")

        if self._token_provider is None:
            self._token_provider = GcpOAuthTokenProvider()

        cfg = {
            "bootstrap.servers":    bootstrap,
            "security.protocol":    security_protocol,
            "sasl.mechanisms":      sasl_mechanism,
            "oauth_cb":             self._token_provider.get_token,
            # librdkafka ships its own OpenSSL whose default CA path differs
            # from the system path — point it at the Debian CA bundle.
            "ssl.ca.location":      "/etc/ssl/certs/cacert.pem",
            # GCP Managed Kafka exposes no AAAA records → force IPv4.
            "broker.address.family": "v4",
        }
        self.log.info(
            "Kafka config: bootstrap=%s, protocol=%s, mechanism=%s",
            bootstrap, security_protocol, sasl_mechanism,
        )
        return cfg

    def _get_admin(self, instance):
        if self._admin is None:
            self._admin = AdminClient(self._build_kafka_config(instance))
            # AdminClient never calls rd_kafka_poll() internally, so the
            # OAUTHBEARER oauth_cb is never triggered until we poll explicitly.
            self._admin.poll(5000)
        return self._admin

    def _get_consumer(self, instance):
        if self._consumer is None:
            cfg = self._build_kafka_config(instance)
            cfg["group.id"]           = "datadog-kafka-consumer-gcp-check"
            cfg["enable.auto.commit"] = False
            self._consumer = Consumer(cfg)
            # Same OAUTHBEARER bootstrap requirement as AdminClient.
            self._consumer.poll(1.0)
        return self._consumer

    # ── Cluster / broker / topic metrics ─────────────────────────────────────

    def _collect_cluster_and_topic_metrics(self, md, instance, custom_tags):
        """
        Single pass over ClusterMetadata (already fetched) to emit:
          • cluster-level aggregates
          • per-broker partition/topic counts
          • per-topic partition / RF / under-replicated / offline counts
        """
        include_internal = instance.get("include_internal_topics", False)

        # ── Per-broker counters ───────────────────────────────────────────
        broker_leader_parts = defaultdict(int)  # broker_id → leader partition count
        broker_total_parts  = defaultdict(int)  # broker_id → total replica count
        broker_topic_set    = defaultdict(set)  # broker_id → set of topic names

        # Seed with zeroes so brokers with no partitions still appear
        for broker in md.brokers.values():
            _ = broker_leader_parts[broker.id]
            _ = broker_total_parts[broker.id]
            _ = broker_topic_set[broker.id]

        # ── Per-topic / cluster counters ──────────────────────────────────
        total_partitions        = 0
        cluster_under_replicated = 0
        cluster_offline         = 0
        visible_topic_count     = 0

        for topic_name, topic_meta in md.topics.items():
            if not include_internal and topic_name.startswith("__"):
                continue
            if topic_meta.error is not None:
                self.log.debug("Topic %s has error: %s", topic_name, topic_meta.error)
                continue

            visible_topic_count   += 1
            topic_partitions       = len(topic_meta.partitions)
            topic_under_replicated = 0
            topic_offline          = 0
            rf                     = 0

            for part_id, part_meta in topic_meta.partitions.items():
                total_partitions += 1

                if rf == 0:
                    rf = len(part_meta.replicas)

                if len(part_meta.isrs) < len(part_meta.replicas):
                    cluster_under_replicated += 1
                    topic_under_replicated   += 1

                if part_meta.leader < 0:
                    cluster_offline += 1
                    topic_offline   += 1
                else:
                    lid = part_meta.leader
                    broker_leader_parts[lid] += 1
                    broker_topic_set[lid].add(topic_name)

                for replica_id in part_meta.replicas:
                    broker_total_parts[replica_id] += 1
                    broker_topic_set[replica_id].add(topic_name)

            t_tags = custom_tags + [f"topic:{topic_name}"]
            self.gauge("kafka_gcp.topic.partitions",               topic_partitions,       tags=t_tags)
            self.gauge("kafka_gcp.topic.replication_factor",       rf,                     tags=t_tags)
            self.gauge("kafka_gcp.topic.under_replicated_partitions", topic_under_replicated, tags=t_tags)
            self.gauge("kafka_gcp.topic.offline_partitions",       topic_offline,          tags=t_tags)

        # ── Cluster-level gauges ──────────────────────────────────────────
        controller_id = md.controller_id if md.controller_id is not None else -1

        self.gauge("kafka_gcp.cluster.brokers",                    len(md.brokers),         tags=custom_tags)
        self.gauge("kafka_gcp.cluster.topics",                     visible_topic_count,     tags=custom_tags)
        self.gauge("kafka_gcp.cluster.partitions",                 total_partitions,        tags=custom_tags)
        self.gauge("kafka_gcp.cluster.under_replicated_partitions", cluster_under_replicated, tags=custom_tags)
        self.gauge("kafka_gcp.cluster.offline_partitions",         cluster_offline,         tags=custom_tags)
        self.gauge("kafka_gcp.cluster.controller_id",              controller_id,           tags=custom_tags)

        # ── Per-broker gauges ─────────────────────────────────────────────
        for broker_id in broker_leader_parts:
            b_tags = custom_tags + [f"broker_id:{broker_id}"]
            self.gauge("kafka_gcp.broker.leader_partitions", broker_leader_parts[broker_id],     tags=b_tags)
            self.gauge("kafka_gcp.broker.total_partitions",  broker_total_parts[broker_id],      tags=b_tags)
            self.gauge("kafka_gcp.broker.topics_count",      len(broker_topic_set[broker_id]),   tags=b_tags)

    # ── Consumer group state metrics ──────────────────────────────────────────

    def _collect_consumer_group_states(self, admin, instance, custom_tags):
        """
        Lists all consumer groups and emits:
          kafka_gcp.consumer_groups.total         — total group count
          kafka_gcp.consumer_groups.by_state      — count per state (tag: state)
          kafka_gcp.consumer_group.info           — 1 per group (tags: consumer_group, state)
          kafka_gcp.consumer_group.state          — numeric state per group (for alerting)

        With collect_consumer_group_details: true also emits:
          kafka_gcp.consumer_group.members
          kafka_gcp.consumer_group.assigned_partitions
        """
        collect_details = instance.get("collect_consumer_group_details", False)

        try:
            result     = admin.list_consumer_groups(request_timeout=15).result()
            groups     = result.valid
        except Exception as e:
            self.log.warning("list_consumer_groups failed: %s", e)
            return

        if not groups:
            return

        # ── Build state map ───────────────────────────────────────────────
        state_counts = defaultdict(int)
        group_states = {}  # group_id → state_name

        for group in groups:
            raw_state = getattr(group, "state", None)
            if raw_state is not None:
                # ConsumerGroupState enum — .name gives "STABLE", "DEAD", etc.
                state_name = getattr(raw_state, "name", str(raw_state)).lower()
                state_name = state_name.split(".")[-1]  # strip any "consumergroupstate." prefix
            else:
                state_name = "unknown"
            group_states[group.group_id] = state_name
            state_counts[state_name] += 1

        # ── Cluster-level aggregates ──────────────────────────────────────
        self.gauge("kafka_gcp.consumer_groups.total", len(groups), tags=custom_tags)
        for state_name, count in state_counts.items():
            self.gauge("kafka_gcp.consumer_groups.by_state", count,
                       tags=custom_tags + [f"state:{state_name}"])

        # ── Per-group state metrics ───────────────────────────────────────
        for gid, state_name in group_states.items():
            g_tags  = custom_tags + [f"consumer_group:{gid}", f"state:{state_name}"]
            numeric = _CG_STATE_NUMERIC.get(state_name, 0)
            self.gauge("kafka_gcp.consumer_group.info",  1,       tags=g_tags)
            self.gauge("kafka_gcp.consumer_group.state", numeric, tags=custom_tags + [f"consumer_group:{gid}"])

        # ── Optional: describe_consumer_groups for member count ───────────
        if not collect_details:
            return

        group_ids = list(group_states.keys())
        try:
            futures = admin.describe_consumer_groups(group_ids, request_timeout=30)
        except Exception as e:
            self.log.warning("describe_consumer_groups failed: %s", e)
            return

        for gid, future in futures.items():
            try:
                desc    = future.result()
                members = desc.members or []
                assigned = sum(
                    len(getattr(m.assignment, "topic_partitions", []) or [])
                    for m in members
                )
                g_tags = custom_tags + [f"consumer_group:{gid}"]
                self.gauge("kafka_gcp.consumer_group.members",             len(members), tags=g_tags)
                self.gauge("kafka_gcp.consumer_group.assigned_partitions", assigned,     tags=g_tags)
            except Exception as e:
                self.log.debug("describe group %s failed: %s", gid, e)

    # ── Topic record counts (watermark-based) ─────────────────────────────────

    def _collect_topic_record_counts(self, consumer, md, instance, custom_tags):
        """
        Emits ``kafka_gcp.topic.records`` = sum(high_offset - low_offset) per topic.

        Makes one get_watermark_offsets() call per partition, so the number of
        network round-trips equals the total partition count.  Disable with
        ``monitor_record_counts: false`` if this is too slow for large clusters.
        """
        include_internal = instance.get("include_internal_topics", False)
        timeout          = float(instance.get("record_count_timeout", 5))

        topic_records = defaultdict(int)

        for topic_name, topic_meta in md.topics.items():
            if not include_internal and topic_name.startswith("__"):
                continue
            if topic_meta.error is not None:
                continue

            for part_id in topic_meta.partitions:
                try:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(topic_name, part_id),
                        timeout=timeout,
                    )
                    if high >= 0 and low >= 0:
                        topic_records[topic_name] += max(high - low, 0)
                except KafkaException as e:
                    self.log.debug("Watermark failed %s/%d: %s", topic_name, part_id, e)

        for topic_name, count in topic_records.items():
            self.gauge("kafka_gcp.topic.records", count,
                       tags=custom_tags + [f"topic:{topic_name}"])

    # ── Topic log-dir sizes ───────────────────────────────────────────────────

    def _collect_topic_log_dir_sizes(self, admin, md, instance, custom_tags):
        """
        Emits ``kafka_gcp.topic.log_size_bytes`` via AdminClient.describe_log_dirs().

        Only counts the leader replica per partition to avoid double-counting.

        NOTE: GCP Managed Kafka may not support the DescribeLogDirs API.
        Errors are logged as warnings and the metric is silently skipped.
        Enable with ``monitor_log_dir_sizes: true``.
        """
        include_internal = instance.get("include_internal_topics", False)

        all_tps = []
        leaders = {}  # (topic, partition) → leader broker_id

        for topic_name, topic_meta in md.topics.items():
            if not include_internal and topic_name.startswith("__"):
                continue
            if topic_meta.error is not None:
                continue
            for part_id, part_meta in topic_meta.partitions.items():
                all_tps.append(TopicPartition(topic_name, part_id))
                if part_meta.leader >= 0:
                    leaders[(topic_name, part_id)] = part_meta.leader

        if not all_tps:
            return

        try:
            futures = admin.describe_log_dirs(all_tps, request_timeout=30)
        except Exception as e:
            self.log.warning("describe_log_dirs unsupported or failed: %s", e)
            return

        topic_bytes = defaultdict(int)

        for tp, future in futures.items():
            try:
                # Future resolves to {broker_id: DescribeLogDirsTopicPartition}
                broker_info_map = future.result()
                leader_id = leaders.get((tp.topic, tp.partition))

                if leader_id is not None and leader_id in broker_info_map:
                    info = broker_info_map[leader_id]
                elif broker_info_map:
                    info = next(iter(broker_info_map.values()))  # fallback
                else:
                    continue

                size = max(getattr(info, "size", 0) or 0, 0)
                topic_bytes[tp.topic] += size
            except Exception as e:
                self.log.debug("describe_log_dirs partial failure %s/%d: %s",
                               tp.topic, tp.partition, e)

        for topic_name, total_bytes in topic_bytes.items():
            self.gauge("kafka_gcp.topic.log_size_bytes", total_bytes,
                       tags=custom_tags + [f"topic:{topic_name}"])

    # ── Wildcard / glob helpers ───────────────────────────────────────────────

    @staticmethod
    def _is_glob(pattern):
        """Return True if the string contains glob characters."""
        return any(c in str(pattern) for c in "*?[]")

    def _resolve_consumer_groups(self, admin, consumer_groups):
        """
        Expand glob patterns in consumer_groups keys against the
        live list of consumer groups on the cluster.
        Returns a dict  {real_group_name: topic_filter, ...}
        """
        if not any(self._is_glob(g) for g in consumer_groups):
            return consumer_groups  # fast path — no globs

        try:
            result     = admin.list_consumer_groups(request_timeout=15).result()
            all_groups = [g.group_id for g in result.valid]
        except Exception as e:
            self.log.warning("Failed to list consumer groups for wildcard expansion: %s", e)
            return consumer_groups

        self.log.debug("Discovered %d consumer groups on cluster", len(all_groups))

        expanded = {}
        for pattern, topic_filter in consumer_groups.items():
            if self._is_glob(pattern):
                matched = fnmatch.filter(all_groups, pattern)
                self.log.debug("Pattern '%s' matched %d groups: %s", pattern, len(matched), matched)
                for g in matched:
                    if g in expanded and isinstance(expanded[g], dict) and isinstance(topic_filter, dict):
                        expanded[g].update(topic_filter)
                    else:
                        expanded[g] = topic_filter
            else:
                expanded[pattern] = topic_filter

        return expanded

    @staticmethod
    def _topic_matches(topic_name, topic_filter):
        """
        Check whether topic_name passes topic_filter.
        Returns (matches: bool, allowed_partitions: list|None).
        None means all partitions.
        """
        if not topic_filter:
            return True, None

        for pattern, partitions in topic_filter.items():
            if fnmatch.fnmatch(topic_name, pattern):
                return True, (partitions if partitions else None)

        return False, None

    # ── Consumer lag metrics ──────────────────────────────────────────────────

    def _collect_consumer_lag(self, admin, consumer, consumer_groups, custom_tags):
        """
        Collect per-(group, topic, partition) committed offset, high watermark,
        and lag.  consumer_groups supports exact names and glob patterns.
        """
        resolved_groups = self._resolve_consumer_groups(admin, consumer_groups)
        if not resolved_groups:
            self.log.debug("No consumer groups matched after wildcard expansion")
            return

        self.log.debug("Collecting lag for %d groups: %s",
                       len(resolved_groups), list(resolved_groups.keys()))

        for group, topic_filter in resolved_groups.items():
            try:
                futures = admin.list_consumer_group_offsets(
                    [ConsumerGroupTopicPartitions(group)]
                )
                offsets = futures[group].result().topic_partitions
            except Exception as e:
                self.log.warning("Failed to list offsets for group %s: %s", group, e)
                continue

            if not offsets:
                continue

            for tp in offsets:
                matches, allowed_partitions = self._topic_matches(tp.topic, topic_filter)
                if not matches:
                    continue
                if allowed_partitions and tp.partition not in allowed_partitions:
                    continue

                committed = tp.offset
                if committed < 0:
                    continue

                try:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(tp.topic, tp.partition),
                        timeout=10,
                    )
                except KafkaException as e:
                    self.log.warning("Failed to get watermarks for %s/%d: %s",
                                     tp.topic, tp.partition, e)
                    continue

                if high < 0:
                    continue

                lag      = max(high - committed, 0)
                lag_tags = custom_tags + [
                    f"consumer_group:{group}",
                    f"topic:{tp.topic}",
                    f"partition:{tp.partition}",
                ]
                self.gauge("kafka_gcp.consumer_lag",    lag,       tags=lag_tags)
                self.gauge("kafka_gcp.consumer_offset", committed, tags=lag_tags)
                self.gauge("kafka_gcp.broker_offset",   high,      tags=lag_tags)

    # ── Main entry point ──────────────────────────────────────────────────────

    def check(self, instance):
        kafka_connect_str = instance.get("kafka_connect_str")
        if not kafka_connect_str:
            self.warning(
                "'kafka_connect_str' is required — list of bootstrap servers. "
                "Same format as the built-in kafka_consumer check."
            )
            return

        custom_tags = self._build_base_tags(instance)

        monitor_cluster   = instance.get("monitor_cluster_metrics",       True)
        monitor_cg_state  = instance.get("monitor_consumer_group_states", True)
        monitor_records   = instance.get("monitor_record_counts",         True)
        monitor_log_dirs  = instance.get("monitor_log_dir_sizes",         False)
        consumer_groups   = instance.get("consumer_groups",               {})

        # Metadata is needed by cluster/topic metrics, record counts, and log dir sizes.
        need_metadata     = monitor_cluster or monitor_records or monitor_log_dirs
        # A Consumer is needed by lag collection, record counts.
        need_consumer     = bool(consumer_groups) or monitor_records

        try:
            # Drive the librdkafka event loop so periodic OAUTHBEARER token
            # rotation callbacks (~1 h before expiry) can fire.
            if self._admin is not None:
                self._admin.poll(0)
            if self._consumer is not None:
                self._consumer.poll(0.0)

            admin = self._get_admin(instance)

            # Fetch cluster metadata once; reuse across all collectors.
            md = admin.list_topics(timeout=15) if need_metadata else None

            if monitor_cluster and md is not None:
                self._collect_cluster_and_topic_metrics(md, instance, custom_tags)

            if monitor_cg_state:
                self._collect_consumer_group_states(admin, instance, custom_tags)

            if need_consumer:
                consumer = self._get_consumer(instance)

                if consumer_groups:
                    self._collect_consumer_lag(admin, consumer, consumer_groups, custom_tags)

                if monitor_records and md is not None:
                    self._collect_topic_record_counts(consumer, md, instance, custom_tags)

            if monitor_log_dirs and md is not None:
                self._collect_topic_log_dir_sizes(admin, md, instance, custom_tags)

            self.service_check(
                "kafka_consumer_gcp.can_connect",
                AgentCheck.OK,
                tags=custom_tags,
                message="Connected to Managed Kafka via GCP OAuth",
            )

        except Exception as e:
            self.log.error("Check failed: %s\n%s", e, traceback.format_exc())
            self.service_check(
                "kafka_consumer_gcp.can_connect",
                AgentCheck.CRITICAL,
                tags=custom_tags,
                message=str(e),
            )
            # Reset clients so the next run reconnects cleanly.
            self._admin = None
            self._consumer = None
            self._token_provider = None
