# Balsam Framework Roadmap

This document outlines the planned features and improvements for the Balsam ETL orchestration framework, following a modular architecture with a lightweight core and optional add-on packages.

## Core Framework (balsam)

### Phase 1: Library Foundation
- [ ] Turning framework into elixir library
- [ ] Plugin system architecture and behavior definitions
- [ ] Configuration-driven feature enabling/disabling
- [ ] Stable public API for extensibility
- [ ] Core workflow execution and orchestration
- [ ] Basic SQLite storage (lightweight, no external dependencies)

### Phase 2: Core Enhancements
- [ ] Workflow validation and testing tools
- [ ] Hot reloading for workflow changes
- [ ] Enhanced error handling and recovery
- [ ] Basic CLI tool for workflow management
- [ ] Documentation generation from workflow configs

## Add-on Packages

### balsam_web - Web Interface
- [ ] UI for tracking jobs
- [ ] Login and authentication
- [ ] UI for viewing jobs and job files
- [ ] Job dependency visualization
- [ ] Real-time job logs streaming

### balsam_scheduler - Advanced Scheduling
- [ ] Cron-based job scheduling
- [ ] Job priority and resource management
- [ ] Workflow templates and reusable components
- [ ] Dynamic workflow generation from configuration files

### balsam_postgres - PostgreSQL Backend
- [ ] Support for PostgreSQL backend
- [ ] Job result caching and storage
- [ ] Data lineage tracking
- [ ] Backup and restore functionality

### balsam_notifications - Messaging & Alerts
- [ ] Notification hooks for Slack
- [ ] Webhook support for job events
- [ ] Email notifications
- [ ] Custom notification channels

### balsam_metrics - Monitoring & Observability
- [ ] Metrics integration (Prometheus/Grafana)
- [ ] Health checks and system monitoring
- [ ] Job execution history and analytics
- [ ] Performance profiling and optimization reports

### balsam_cloud - Cloud Integrations
- [ ] Integration with cloud storage (AWS S3, GCS, Azure Blob)
- [ ] Integration with message queues (RabbitMQ, Apache Kafka)
- [ ] Cloud-native deployment configurations
- [ ] Kubernetes operators

### balsam_security - Security & Compliance
- [ ] Role-based access control (RBAC)
- [ ] Audit logging
- [ ] Data encryption at rest and in transit
- [ ] Secret management integration (HashiCorp Vault)

### balsam_scale - Performance & Scalability
- [ ] Distributed job execution across nodes
- [ ] Job queue optimization
- [ ] Resource usage monitoring and limits
- [ ] Auto-scaling capabilities
- [ ] Load balancing for high-throughput scenarios

### balsam_dev - Developer Experience
- [ ] VS Code extension for workflow development
- [ ] Advanced workflow testing and validation tools
- [ ] Development workflow hot-reloading
- [ ] Workflow debugging tools

## Implementation Strategy

1. **Start Small**: Focus on core framework as minimal, stable foundation
2. **Plugin Architecture**: Implement behavior-based plugin system early
3. **Community-Driven**: Enable community to build and maintain add-ons
4. **Gradual Growth**: Users can add complexity only as needed
5. **Stable APIs**: Maintain backward compatibility in core interfaces

This modular approach ensures Balsam remains lightweight and accessible while providing a path to enterprise-grade features through optional add-ons.