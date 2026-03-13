# ─────────────────────────────────────────────────────────────
# Custom Datadog Agent with Google OAuth for Managed Kafka.
#
# Adds:
#   - google-auth    (ADC / Workload Identity)
#   - confluent-kafka (SASL/OAUTHBEARER client)
#   - kafka_consumer_gcp.py custom check (baked in)
#
# Check CONFIG is injected via the Helm chart (datadog.confd).
# ─────────────────────────────────────────────────────────────
ARG DD_AGENT_VERSION=7
FROM gcr.io/datadoghq/agent:${DD_AGENT_VERSION}

RUN /opt/datadog-agent/embedded/bin/pip install --no-cache-dir \
    confluent-kafka \
    google-auth \
    urllib3 \
    packaging

COPY checks.d/kafka_consumer_gcp.py /etc/datadog-agent/checks.d/kafka_consumer_gcp.py
