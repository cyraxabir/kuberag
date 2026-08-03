"""
k8s-watcher: watches the Kubernetes API and publishes resource change events
to the Kafka topic `k8s-resources`.

Each event payload:
  {event_type, resource_uid, kind, namespace, name, labels, annotations,
   raw_spec_json, last_updated_timestamp}

resource_uid is a UUID and is used directly as the Qdrant point ID.
"""
import json
import logging
import os
import threading
import time
import urllib3
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from kubernetes import client, config, watch
from kafka import KafkaProducer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("k8s-watcher")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "k8s-resources")
KUBECONFIG = os.getenv("KUBECONFIG", "/root/.kube/config")
# Override the API server address (needed because kind binds to 127.0.0.1
# which is unreachable from inside Docker; use host.docker.internal instead).
K8S_SERVER = os.getenv("K8S_SERVER", "")


def load_k8s():
    cfg = client.Configuration()
    config.load_kube_config(config_file=KUBECONFIG, client_configuration=cfg)
    if K8S_SERVER:
        cfg.host = K8S_SERVER
        cfg.verify_ssl = False  # cert is bound to 127.0.0.1, not the override host
    client.Configuration.set_default(cfg)
    log.info("K8s client configured — server: %s", cfg.host)


def make_producer() -> KafkaProducer:
    for attempt in range(1, 13):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                acks="all",
            )
            log.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP)
            return p
        except Exception as exc:
            log.warning("Kafka not ready (%s) — attempt %d/12, retrying in 5s", exc, attempt)
            time.sleep(5)
    raise RuntimeError(f"Cannot connect to Kafka at {KAFKA_BOOTSTRAP}")


def obj_to_payload(event_type: str, obj) -> dict:
    meta = obj.metadata
    raw = obj.to_dict()
    spec = raw.get("spec") or {}
    kind = obj.kind or raw.get("kind", "")
    namespace = meta.namespace or ""
    name = meta.name or ""
    labels = meta.labels or {}
    annotations = {
        k: v
        for k, v in (meta.annotations or {}).items()
        if not k.startswith("kubectl.kubernetes.io/last-applied")
    }
    spec_json = json.dumps(spec, default=str)[:4000]

    # Natural-language embed text improves semantic similarity for RAG queries.
    # e.g. "What deployments exist?" → matches "Kubernetes Deployment coredns..."
    scope = f"in namespace {namespace}" if namespace else "cluster-scoped"
    label_str = ", ".join(f"{k}={v}" for k, v in list(labels.items())[:5]) or "none"
    embed_text = (
        f"Kubernetes {kind} named {name} {scope}. "
        f"Labels: {label_str}. "
        f"Spec: {spec_json[:600]}"
    )

    return {
        "event_type": event_type,          # ADDED | MODIFIED | DELETED
        "resource_uid": meta.uid,
        "kind": kind,
        "api_version": obj.api_version or raw.get("apiVersion", ""),
        "namespace": namespace,
        "name": name,
        "labels": labels,
        "annotations": annotations,
        "raw_spec_json": spec_json,
        "embed_text": embed_text,
        "last_updated_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def watch_stream(list_fn, label: str, producer: KafkaProducer):
    """Runs a perpetual watch loop for a single resource type."""
    while True:
        w = watch.Watch()
        try:
            log.info("Starting watch: %s", label)
            for event in w.stream(list_fn, timeout_seconds=0):
                obj = event["object"]
                if not obj.metadata or not obj.metadata.uid:
                    continue
                payload = obj_to_payload(event["type"], obj)
                producer.send(KAFKA_TOPIC, payload)
                log.info(
                    "%-9s %-20s ns=%-18s uid=%s",
                    payload["event_type"],
                    payload["kind"] + "/" + payload["name"],
                    payload["namespace"] or "<cluster>",
                    payload["resource_uid"],
                )
        except Exception as exc:
            log.error("Watch %s error: %s — restarting in 5s", label, exc)
            time.sleep(5)


RESYNC_PORT = int(os.getenv("RESYNC_PORT", "8080"))


def resync_all(v1: client.CoreV1Api, apps: client.AppsV1Api, producer: KafkaProducer):
    """List every tracked resource type and publish ADDED events to Kafka."""
    resource_fns = [
        (v1.list_namespace,                                   "Namespace"),
        (v1.list_pod_for_all_namespaces,                      "Pod"),
        (v1.list_service_for_all_namespaces,                  "Service"),
        (v1.list_config_map_for_all_namespaces,               "ConfigMap"),
        (v1.list_persistent_volume_claim_for_all_namespaces,  "PVC"),
        (apps.list_deployment_for_all_namespaces,             "Deployment"),
        (apps.list_replica_set_for_all_namespaces,            "ReplicaSet"),
        (apps.list_stateful_set_for_all_namespaces,           "StatefulSet"),
        (apps.list_daemon_set_for_all_namespaces,             "DaemonSet"),
    ]
    total = 0
    for list_fn, label in resource_fns:
        try:
            items = list_fn().items
            for obj in items:
                if not obj.metadata or not obj.metadata.uid:
                    continue
                payload = obj_to_payload("ADDED", obj)
                producer.send(KAFKA_TOPIC, payload)
                total += 1
            log.info("Resync: published %d %s resources", len(items), label)
        except Exception as exc:
            log.error("Resync error listing %s: %s", label, exc)
    producer.flush()
    log.info("Resync complete — %d events published to '%s'", total, KAFKA_TOPIC)


def make_resync_handler(v1: client.CoreV1Api, apps: client.AppsV1Api, producer: KafkaProducer):
    """Return an HTTPRequestHandler class bound to the given K8s/Kafka clients."""

    class ResyncHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            log.info("HTTP %s", fmt % args)

        def do_GET(self):
            if self.path == "/healthz":
                body = b'{"status":"ok"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            if self.path == "/resync":
                # Acknowledge immediately; run resync in background
                body = b'{"status":"accepted","message":"Resync started in background"}'
                self.send_response(202)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                threading.Thread(
                    target=resync_all,
                    args=(v1, apps, producer),
                    daemon=True,
                ).start()
            else:
                self.send_response(404)
                self.end_headers()

    return ResyncHandler


def start_resync_server(v1: client.CoreV1Api, apps: client.AppsV1Api, producer: KafkaProducer):
    """Start the resync HTTP server on RESYNC_PORT in a daemon thread."""
    handler = make_resync_handler(v1, apps, producer)
    server = HTTPServer(("0.0.0.0", RESYNC_PORT), handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("Resync HTTP server listening on port %d", RESYNC_PORT)


def main():
    load_k8s()
    producer = make_producer()

    v1 = client.CoreV1Api()
    apps = client.AppsV1Api()

    start_resync_server(v1, apps, producer)

    watchers = [
        (v1.list_namespace,                              "Namespace"),
        (v1.list_pod_for_all_namespaces,                 "Pod"),
        (v1.list_service_for_all_namespaces,             "Service"),
        (v1.list_config_map_for_all_namespaces,          "ConfigMap"),
        (v1.list_persistent_volume_claim_for_all_namespaces, "PVC"),
        (apps.list_deployment_for_all_namespaces,        "Deployment"),
        (apps.list_replica_set_for_all_namespaces,       "ReplicaSet"),
        (apps.list_stateful_set_for_all_namespaces,      "StatefulSet"),
        (apps.list_daemon_set_for_all_namespaces,        "DaemonSet"),
    ]

    threads = []
    for list_fn, label in watchers:
        t = threading.Thread(
            target=watch_stream,
            args=(list_fn, label, producer),
            daemon=True,
        )
        t.start()
        threads.append(t)

    log.info("Watching %d resource types on topic '%s'", len(threads), KAFKA_TOPIC)
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
