# 🏢 MaeSoonGan On-Prem Main Server

## ✍️ 프로젝트 한 줄 소개

- 한국 금융권 이중 IDC 규제를 반영한 모의투자 서비스에서, 거래의 핵심인 계정계(체결엔진·원장시스템)와 DB HA 스택을 운영하는 On-Premise 주센터 서버입니다. MySQL Semi-sync 복제와 자동 페일오버를 기반으로 무중단 거래 처리와 데이터 정합성을 보장합니다.

<br />

## 🛫 레포지토리 개요

- 이 레포지토리는 MaeSoonGan 프로젝트의 **주센터(Main Center) 서버** 구축 및 운영 문서를 관리합니다.

- 주센터는 모의투자 서비스의 계정계(체결엔진·원장시스템)를 전담하는 On-Premise 영역입니다. 금융권 계정계는 데이터 정합성과 무중단성이 가장 중요하므로 클라우드가 아닌 온프레미스 전용으로 배치했고, 채널계(실시간 시세·사용자 트래픽)는 AWS EKS에 분리했습니다. Core Cluster는 계정계 서버·체결 엔진·Kafka를, DB Cluster는 Master/Slave 데이터베이스를, CI/CD Cluster는 빌드·배포 파이프라인을 담당합니다.

- DB 영역은 MySQL Semi-sync 복제를 기반으로 Orchestrator 자동 페일오버와 ProxySQL 읽기/쓰기 라우팅을 구성하여 주센터 내부 장애 시 RPO ≈ 0을 목표로 합니다. 체결 엔진은 KIS API 시세를 연동하고, Kafka 기반 주문 이벤트를 멱등성 설계로 처리합니다. CI/CD 영역은 GitLab을 기반으로 정적분석·아티팩트·이미지 관리·배포를 일괄 수행합니다.

<br />

## 🖥️ 서버 사양

| 구분         | 내용              |
| ---------- | --------------- |
| Hypervisor | VMware ESXi 7.0U3 |
| OS         | ESXi            |
| vCPU       | 16              |
| RAM        | 64GB            |
| Disk       | 1TB           |

<br />

## 🧩 구성 요소

| 구성 요소           | 역할                                            |
| --------------- | --------------------------------------------- |
| VyOS Router     | 주센터 서버 내 VLAN 간 내부 라우팅 (라우팅 분산)               |
| 계정계 서버 (원장시스템)  | 단일 진실 소스(Single Source of Truth) 원장 DB 운영     |
| 체결 엔진           | 주문 체결 처리, KIS API 시세 연동, 멱등성 기반 주문 상태 관리      |
| Kafka           | 주문 이벤트 스트리밍 (at-least-once, 멱등 컨슈머 대응)        |
| MySQL Master    | Semi-sync 복제의 Primary 노드 (읽기/쓰기)              |
| MySQL Slave     | Semi-sync Replica 노드 (읽기 부하 분산)               |
| Orchestrator    | MySQL 토폴로지 관리 및 자동 페일오버                        |
| ProxySQL        | 읽기/쓰기 트래픽 라우팅, Cluster 설정 동기화                  |
| GitLab          | 소스 관리 및 CI 파이프라인                               |
| Onprem-runner   | GitLab CI 작업 실행 러너                             |
| Vault           | 서비스 시크릿(client secret) 중앙 관리                   |
| Ansible AWX     | 구성 관리 및 배포 자동화                                 |
| SonarQube       | 정적 코드 분석 및 품질 게이트                              |
| Nexus           | 빌드 아티팩트 저장소                                    |
| Harbor          | 컨테이너 이미지 레지스트리                                 |

<br />

## 🏗️ 주센터 서버 아키텍처

<img width="513" height="379" alt="image" src="https://github.com/user-attachments/assets/af141fe5-fbc2-4eee-a019-baaee1e6ab13" />


<br />

## 📡 주요 구성 영역

### Core Cluster

* 계정계 서버 (원장시스템) — 거래 원장 단일 진실 소스
* 체결 엔진 — 주문 접수·체결 처리, KIS API 시세 연동
* Kafka — 주문 이벤트 스트리밍

### DB Cluster

* MySQL Master / Slave (Semi-sync Replication)
* Orchestrator — 자동 페일오버
* ProxySQL — 읽기/쓰기 라우팅

### CI/CD Cluster

* GitLab + Onprem-runner — CI 파이프라인
* SonarQube — 정적 분석
* Nexus / Harbor — 아티팩트 및 이미지 관리
* Vault / Ansible AWX — 시크릿 관리 및 배포 자동화

<br />

## 🔁 데이터 흐름

1. 사용자의 주문 요청이 채널계(AWS EKS)를 거쳐 주센터 체결 엔진으로 전달됩니다.
2. 체결 엔진은 KIS API로부터 시세를 받아 주문 체결 여부를 판단합니다.
3. 체결 결과는 주문 이벤트로 Kafka에 발행됩니다.
4. 원장시스템은 Kafka 이벤트를 멱등성 기반으로 소비하여 원장 DB에 기록합니다.
5. 원장 DB는 MySQL Master에 쓰기되고, Semi-sync 복제로 Slave에 동기화됩니다.
6. ProxySQL은 읽기 트래픽을 Slave로, 쓰기 트래픽을 Master로 라우팅합니다.
7. Orchestrator가 Master 장애를 감지하면 자동 페일오버를 수행합니다.
8. 애플리케이션 소스는 GitLab에 푸시되어 CI 파이프라인이 실행됩니다.
9. SonarQube 품질 게이트 통과 후 아티팩트는 Nexus, 이미지는 Harbor에 저장됩니다.
10. Vault에서 시크릿을 주입받아 빌드·배포가 수행됩니다.

<br />

## 📋 목차

### 가상화 / 스토리지

* [vDS(Distributed Switch) 설계](./docs/virtualization/vds-design.md)
* [TrueNAS iSCSI 스토리지 구성](./docs/storage/truenas-iscsi.md)

### Core Cluster

* [체결 엔진 구성](./docs/core/matching-engine.md)
* [원장시스템 구성](./docs/core/ledger-system.md)
* [Kafka 구성](./docs/core/kafka.md)

### DB HA

* [MySQL Semi-sync 복제 구성](./docs/db/mysql-semisync.md)
* [Orchestrator 자동 페일오버](./docs/db/orchestrator.md)
* [ProxySQL 라우팅 구성](./docs/db/proxysql.md)

### CI/CD Cluster

* [GitLab CI 파이프라인](./docs/cicd/gitlab.md)
* [SonarQube 품질 게이트](./docs/cicd/sonarqube.md)
* [Nexus / Harbor 구성](./docs/cicd/nexus-harbor.md)
* [Vault / Ansible AWX 구성](./docs/cicd/vault-awx.md)
