# kube-rag
🤖 Real-time Kubernetes AI assistant — indexes cluster resources via Kafka into Qdrant vector DB, answers natural language queries using local LLMs (Ollama/qwen3) or ChatGPT. Built with n8n, Kafka, Qdrant, and Telegram integration.

# 🤖 KubeRAG — AI-Powered Kubernetes Monitoring System

> Ask your Kubernetes cluster anything in plain language. Get instant, real-time answers powered by local LLMs.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://docs.docker.com/compose/)
[![n8n](https://img.shields.io/badge/Automation-n8n-orange)](https://n8n.io/)
[![Qdrant](https://img.shields.io/badge/VectorDB-Qdrant-red)](https://qdrant.tech/)

---

## 📖 What is KubeRAG?

**KubeRAG** is a real-time Retrieval-Augmented Generation (RAG) system for Kubernetes clusters. It continuously indexes your cluster resources into a vector database and lets you query them using natural language — via chat UI or Telegram bot.

```
"Which pods are not running in the argocd namespace?"
                        ↓
              KubeRAG answers instantly
using real cluster data, no kubectl required.
```

---

## ✨ Features

- 🔄 **Real-time indexing** — cluster changes indexed automatically via Kafka CDC pipeline
- 💬 **Natural language queries** — ask in plain English, get structured answers
- 📱 **Telegram bot** — query your cluster from anywhere on your phone
- 🧠 **Local LLM support** — runs with Ollama (free, private, no cloud)
- 🔌 **Multi-LLM** — switch between Ollama, ChatGPT, or Groq with one config change
- 🔒 **Privacy-first** — all data stays on your infrastructure
- ⚡ **Event-driven** — zero polling, instant updates via Kubernetes watch API

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    WRITE PIPELINE                           │
│                                                             │
│  Kubernetes API                                             │
│       ↓ (kubeconfig)                                        │
│  k8s-watcher  ──────→  Kafka  ──────→  n8n CDC Flow        │
│  (watches 9        (buffers          (embeds + indexes)     │
│   resource types)   events)               ↓                 │
│                                       Ollama API            │
│                                    (nomic-embed-text)       │
│                                           ↓                 │
│                                       Qdrant DB             │
│                                    (768-dim vectors)        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    QUERY PIPELINE                           │
│                                                             │
│  User (Telegram / Chat UI)                                  │
│       ↓                                                     │
│  n8n AI Flow                                                │
│       ↓ embed question                                      │
│  Ollama API (nomic-embed-text)                              │
│       ↓ similarity search                                   │
│  Qdrant DB (top 30 results)                                 │
│       ↓ build prompt                                        │
│  LLM (qwen3:8b / GPT-4o / Groq)                            │
│       ↓                                                     │
│  Human-readable answer                                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 🧩 Stack

| Component | Technology | Role |
|---|---|---|
| **Workflow Engine** | [n8n](https://n8n.io/) | Orchestrates all pipelines |
| **Message Queue** | [Apache Kafka](https://kafka.apache.org/) | Buffers K8s events |
| **Vector Database** | [Qdrant](https://qdrant.tech/) | Stores resource embeddings |
| **Embedding Model** | nomic-embed-text (Ollama) | Text → 768-dim vectors |
| **LLM** | qwen3:8b / GPT-4o / Groq | Generates answers |
| **K8s Watcher** | Custom Python service | Watches cluster via kubeconfig |
| **Chat Interface** | Telegram Bot / n8n Chat | User-facing interface |

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Access to a Kubernetes cluster (kubeconfig)
- Ollama server with `nomic-embed-text` and `qwen3:8b` models

### 1. Clone the repository

```bash
git clone https://github.com/cyraxabir/kube-rag.git
cd kuberag
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your settings
```

### 3. Set up kubeconfig

```bash
mkdir -p ~/.kube
cp your-cluster-kubeconfig ~/.kube/config
```

### 4. Fix permissions and start

```bash
mkdir -p data/kafka data/qdrant data/n8n
sudo chown -R 1000:1000 data/

docker compose up -d
```

### 5. Import n8n workflows

```bash
# Wait for n8n to start
sleep 20

docker cp workflows/CDC_K8s_Flow.json   kuberag-n8n-1:/tmp/
docker cp workflows/AI_K8s_Flow.json    kuberag-n8n-1:/tmp/
docker cp workflows/Reset_K8s_Flow.json kuberag-n8n-1:/tmp/

docker exec kuberag-n8n-1 n8n import:workflow --input=/tmp/CDC_K8s_Flow.json
docker exec kuberag-n8n-1 n8n import:workflow --input=/tmp/AI_K8s_Flow.json
docker exec kuberag-n8n-1 n8n import:workflow --input=/tmp/Reset_K8s_Flow.json
```

### 6. Configure n8n credentials

Open n8n at `http://localhost:5678` and create:

- **Kafka** credential → brokers: `kafka:9092`
- **HTTP Bearer Auth** → your Ollama/OpenWebUI API token

Activate all 3 workflows, then trigger initial index:

```bash
curl -X POST http://localhost:5678/webhook/k8s-reset
```

### 7. Ask your first question

```bash
curl -s http://localhost:5678/webhook/k8s-ai-chat/chat \
  -X POST -H "Content-Type: application/json" \
  -d '{"message":"list all namespaces"}'
```

---

## 📁 Project Structure

```
kuberag/
├── docker-compose.yml          # All services
├── .env.example                # Environment template
├── k8s-watcher/                # Kubernetes watch service
│   ├── Dockerfile
│   ├── watcher.py
│   └── requirements.txt
├── workflows/                  # n8n workflow exports
│   ├── CDC_K8s_Flow.json       # Indexing pipeline
│   ├── AI_K8s_Flow.json        # Query pipeline
│   └── Reset_K8s_Flow.json     # Re-index pipeline
├── data/                       # Persistent volumes (gitignored)
│   ├── kafka/
│   ├── qdrant/
│   └── n8n/
└── docs/
    └── architecture.png
```

---

## 🔧 Configuration

### Environment Variables (`.env`)

```env
# n8n
N8N_BASIC_AUTH_USER=admin
N8N_BASIC_AUTH_PASSWORD=changeme
N8N_HOST=your-domain.com
N8N_PROTOCOL=https

# Ollama
OLLAMA_URL=https://your-ollama-server/ollama/api/embeddings
OLLAMA_BEARER_TOKEN=your-token
LLM_URL=https://your-ollama-server/api/v1/chat/completions
LLM_MODEL=qwen3:8b

# Qdrant
QDRANT_HOST=<server-ip>
QDRANT_PORT=6333
```

### Switching LLM Providers

In the **AI_K8s_Flow** → **LLM Chat** node, change the URL and model:

| Provider | URL | Model |
|---|---|---|
| Ollama (local) | `https://your-ollama/api/v1/chat/completions` | `qwen3:8b` |
| Groq (free) | `https://api.groq.com/openai/v1/chat/completions` | `llama-3.3-70b-versatile` |
| OpenAI | `https://api.openai.com/v1/chat/completions` | `gpt-4o-mini` |

---

## 💬 Example Queries

```
"list all namespaces"
"which pods are not running?"
"show all deployments in the argocd namespace"
"which services expose port 8080?"
"what Helm releases are installed?"
"are there any deployments with 0 replicas?"
"show all resources in monitoring namespace"
```

---

## 🔁 Workflows

| Workflow | Trigger | Purpose |
|---|---|---|
| **CDC_K8s_Flow** | Kafka message | Auto-index cluster changes |
| **AI_K8s_Flow** | Chat / Telegram | Answer natural language questions |
| **Reset_K8s_Flow** | POST /webhook/k8s-reset | Wipe and re-index all resources |

---

## 🌐 Nginx Configuration (Optional)

To expose n8n via HTTPS domain:

```nginx
server {
    listen 443 ssl;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:5678;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
    }
}
```

Add to n8n environment:
```yaml
- N8N_HOST=your-domain.com
- N8N_PROTOCOL=https
- N8N_PUSH_BACKEND=sse
- N8N_PROXY_HOPS=1
```

---

## 🛠️ Maintenance

```bash
# Check all services
docker compose ps

# View logs
docker compose logs -f n8n
docker compose logs -f k8s-watcher

# Check indexed points
curl -s http://localhost:6333/collections/k8s | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print('points:', d['result']['points_count'])"

# Full re-index
curl -X POST http://localhost:5678/webhook/k8s-reset

# Reset Kafka consumer offset
docker compose stop n8n
docker exec kuberag-kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group n8n-cdc-consumer \
  --topic k8s-resources \
  --reset-offsets --to-earliest --execute
docker compose start n8n
```

---

## 📋 Watched Resource Types

| Resource | API Group |
|---|---|
| Pods | v1 |
| Services | v1 |
| ConfigMaps | v1 |
| Namespaces | v1 |
| Deployments | apps/v1 |
| StatefulSets | apps/v1 |
| DaemonSets | apps/v1 |
| ReplicaSets | apps/v1 |
| PersistentVolumeClaims | v1 |

---

## 🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 🙏 Built With

- [n8n](https://n8n.io/) — Workflow automation
- [Qdrant](https://qdrant.tech/) — Vector database
- [Apache Kafka](https://kafka.apache.org/) — Event streaming
- [Ollama](https://ollama.ai/) — Local LLM runtime
- [nomic-embed-text](https://www.nomic.ai/) — Text embeddings

---

*Built by the Infra Team — "Automate everything. Question anything."*