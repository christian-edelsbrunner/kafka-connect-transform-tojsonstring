# Changelog

All notable changes to this project will be documented in this file.

## [2.0] - 2026-04-08

### Breaking Changes
- **Java 17 required**: Minimum Java version raised from 8 to 17. Kafka Connect 3.x+ with Java 17 runtime is required.
- **Project version bumped to 2.0** to reflect breaking Java version change.

### Security Fixes
- **Apache Avro** upgraded from 1.11.1 to 1.12.0 — fixes CVE for improper input validation (patched 1.11.3) and arbitrary code execution when reading Avro data (patched 1.11.4).
- **PostgreSQL JDBC Driver** upgraded from 42.5.0 to 42.7.3 — fixes multiple CVEs for SQL injection via line comment generation.

### Dependency Upgrades
- Apache Kafka Connect API: 3.3.1 → 3.9.0
- Confluent kafka-avro-serializer: 7.2.2 → 7.9.0
- MongoDB BSON: 4.8.0 → 4.11.0
- Logback: 1.4.14 → 1.5.6
- SLF4J Simple: 2.0.9 → 2.0.13
- Underscore (javadev): 1.81 → 1.100
- JUnit Jupiter: 5.5.2 → 5.10.2
- JUnit Platform: 1.5.2 → 1.10.2
- Mockito: 3.2.4 → 5.11.0
- OkHttp: 4.10.0 → 4.12.0
- Jackson Annotations: 2.13.4 → 2.17.0
- Testcontainers: 1.19.3 → 1.19.7
- Yamlbeans: 1.15 → 1.17

### Maven Plugin Upgrades
- JaCoCo: 0.8.6 → 0.8.12
- maven-compiler-plugin: 3.8.1 → 3.13.0
- maven-jar-plugin: 3.2.0 → 3.4.1
- maven-assembly-plugin: 3.2.0 → 3.7.1
- maven-release-plugin: 2.5.3 → 3.1.0
- maven-failsafe-plugin: 2.22.2 → 3.2.5

### CI/CD Improvements
- GitHub Actions: Updated all action versions (checkout v4, setup-java v4, cache v4)
- Build workflow now uses JDK 17 (previously JDK 11 / JDK 8)
- Trivy scanner pinned to release tag v0.28.0 (was @master)
- CodeQL upload-sarif updated to v3
- Added Dependabot configuration for automated Maven and GitHub Actions dependency updates

### Project Hygiene
- Updated all repository URLs from `an0r0c/` to `christian-edelsbrunner/`
- Updated Confluent Hub plugin metadata URLs

## [1.4] - 2023-12-04

### Changes
- Fix integration tests
- Upgrade dependencies with identified vulnerabilities
- Fix Sonar issues

## [1.3] - Previous

### Changes
- Added support for date/time logical type string formatting
- Various improvements
