# OpenSearch Toolkit 🛠️

This repository is a comprehensive collection of scripts and tools designed to streamline the management, monitoring, and data resilience of OpenSearch clusters, with a specific focus on **Graylog (v5 & v6)** environments.

## 📂 Repository Structure

The project is organized by operational domains to ensure quick access to the right tool:

* **`backup_graylog_v6 / backup_graylog_v5`**: Specialized scripts for snapshot automation and cluster backups tailored to specific Graylog version APIs.
* **`monitoring`**: Tools for health checks, shard allocation monitoring, and cluster performance auditing.
* **`restore`**: Procedures and automation scripts for data recovery and snapshot restoration.
* **`delete`**: Automation for data retention policies, index curation, and cleanup tasks.

## 🚀 Getting Started

### Prerequisites
* Python 3.x or Bash (depending on the script).
* Network access to the OpenSearch cluster endpoint.
* Proper IAM/Role permissions for snapshot and cluster management.

### Installation
Clone the repository to your local machine or management server:
```bash
git clone https://github.com/ItamarMesquita/opensearch-toolkit.git
cd opensearch-toolkit
```

## ⚙️ Configuration

Most scripts are designed to read cluster credentials from environment variables to keep your credentials secure:

```bash
export OPENSEARCH_HOST='https://your-cluster-endpoint:9200'
export OPENSEARCH_USER='admin'
export OPENSEARCH_PASS='your-secure-password'
```

## 🛠️ Operational Details

### Backup & Restore
These directories contain scripts that automate the interaction with the OpenSearch Snapshot API. The separation between `v5` and `v6` ensures compatibility with specific Graylog index set behaviors and API changes between versions.

### Monitoring
Essential for production environments. These scripts help identify "Unassigned Shards," "Red" cluster states, and node resource exhaustion before they impact log ingestion.

## 🤝 Contributing

1.  **Fork** the project.
2.  Create your **Feature Branch** (`git checkout -b feature/amazing-script`).
3.  **Commit** your changes (`git commit -m 'Add some amazing script'`).
4.  **Push** to the branch (`git push origin feature/amazing-script`).
5.  Open a **Pull Request**.

---

**Maintained by:** [Itamar Mesquita](https://github.com/ItamarMesquita)