# Apache Kafka 구성

## 1. 개요 및 프로젝트 내 역할

### 1.1 구성 목적

MaeSoonGan 프로젝트에서는 주문 서비스, 체결 엔진, 계정계 시스템 및 데이터 동기화 Worker 사이의 이벤트를 비동기적으로 전달하기 위해 Apache Kafka를 구성했습니다.

주문 요청과 체결 결과를 HTTP 기반 동기 통신만으로 처리하면 체결 엔진이나 계정계 시스템의 응답 지연 및 장애가 호출한 서비스까지 전파될 수 있습니다.

이를 방지하기 위해 Kafka를 메시지 브로커로 사용하여 각 시스템을 비동기적으로 연결했습니다.

```text
order-service
    │
    │ 주문·취소 이벤트 발행
    ▼
Apache Kafka
    │
    │ 이벤트 전달
    ▼
체결 엔진
    │
    │ 체결 결과 발행
    ▼
Apache Kafka
    │
    │ 체결 결과 전달
    ▼
trade-sync-worker
    │
    │ 주문·체결·포트폴리오 데이터 동기화
    ▼
Database
```

Kafka는 프로젝트에서 다음 역할을 담당합니다.

- 주문 요청 이벤트 전달
- 주문 취소 요청 이벤트 전달
- 체결 결과 이벤트 전달
- 계정계 및 회원 명령 결과 전달
- 서비스 간 결합도 감소
- Consumer 장애 시 메시지 임시 보관
- Consumer Group 기반 병렬 처리
- Offset 기반 이벤트 재처리 지원

---

### 1.2 프로젝트 내 위치

Kafka는 온프레미스 Core Cluster 내부의 별도 VM에 배치했습니다.

```text
[order-service]
AWS EKS 또는 계정계 애플리케이션
        │
        │ order.request
        │ order.cancel
        ▼
[Kafka VM]
10.4.10.33:9092
        │
        ▼
[체결 엔진]
10.4.10.22
        │
        │ execution.confirmed
        ▼
[Kafka VM]
10.4.10.33:9092
        │
        ▼
[trade-sync-worker]
        │
        ▼
주문·체결·포트폴리오 데이터 동기화
```

계정계 서버, 체결 엔진 및 Kafka VM은 VLAN 10 네트워크에 배치하여 내부 통신이 가능하도록 구성했습니다.

---

### 1.3 Kafka Topic 구성

현재 백엔드 설정을 기준으로 사용하는 Topic은 다음과 같습니다.

| Topic | 역할 | Producer | Consumer |
|---|---|---|---|
| `order.request` | 주문 요청 전달 | `order-service` | 체결 엔진 |
| `order.cancel` | 주문 취소 요청 전달 | `order-service` | 체결 엔진 |
| `execution.confirmed` | 체결 결과 전달 | 체결 엔진 | `trade-sync-worker` |
| `member.command.result` | 회원·계정계 명령 처리 결과 전달 | 계정계 또는 회원 서비스 | `trade-sync-worker` |

구현되지 않았거나 실제 코드에서 사용하지 않는 Topic은 문서에 포함하지 않습니다.

각 Topic은 기본적으로 Partition 3개로 구성합니다.

```text
Topic
├── Partition 0
├── Partition 1
└── Partition 2
```

Partition을 여러 개로 구성하면 Consumer Group 내 Consumer를 추가하여 메시지를 병렬로 처리할 수 있습니다.

주문 이벤트는 주문 ID를 Message Key로 사용하여 동일한 주문과 관련된 메시지가 같은 Partition으로 전달되도록 구성하는 것이 적절합니다.

---

## 2. 구성 환경 및 사양

### 2.1 전체 구성 환경

| 구분 | 구성 |
|---|---|
| 가상화 환경 | VMware vSphere / vCenter |
| VM 이름 | `kafka-vm` |
| 운영체제 | Rocky Linux 9 |
| CPU | 2 Core |
| Memory | 4 GB |
| Disk | 30 GB |
| Network | VLAN 10 |
| 고정 IP | `10.4.10.33/24` |
| Gateway | `10.4.10.1` |
| Container Runtime | Docker |
| 실행 방식 | Docker Compose |
| Kafka Image | `confluentinc/cp-kafka:7.6.0` |
| Kafka UI Image | `provectuslabs/kafka-ui:v0.7.2` |
| Kafka 운영 모드 | KRaft |
| Kafka Broker Port | `9092` |
| KRaft Controller Port | `9093` |
| Kafka UI Port | `8989` |
| Kafka 데이터 저장 | Docker Named Volume `kafka-data` |
| Container 내부 저장 경로 | `/var/lib/kafka/data` |
| 기본 Partition 수 | 3 |
| 메시지 보존 기간 | 168시간 |
| Replication Factor | 1 |
| 통신 방식 | PLAINTEXT |

현재 구성은 내부 VLAN의 개발 및 테스트 환경을 기준으로 하며, Kafka Client 통신에 `PLAINTEXT`를 사용합니다.

```text
인증 없음
전송 구간 암호화 없음
```

운영 환경에서는 SASL 인증과 TLS 암호화를 적용해야 합니다.

---

### 2.2 VM 사양 선정 이유

#### CPU: 2 Core

Kafka는 다음 작업을 동시에 수행합니다.

- Producer 요청 수신
- 메시지 디스크 저장
- Consumer 메시지 전달
- Partition 관리
- Consumer Offset 관리
- KRaft Controller 처리

1 Core만 할당하면 메시지 입출력과 Kafka 내부 관리 작업이 같은 CPU 자원을 두고 경쟁할 수 있습니다.

개발 및 테스트 환경에서 단일 Broker와 Controller를 함께 실행하기 위한 최소 기준으로 2 Core를 할당했습니다.

#### Memory: 4 GB

Kafka는 JVM 기반 애플리케이션이며 JVM Heap뿐 아니라 운영체제의 Page Cache를 적극적으로 사용합니다.

```text
Kafka JVM Heap     약 1~2 GB
Docker 및 OS       일부 사용
Linux Page Cache   나머지 메모리 사용
```

Kafka는 디스크에 저장된 메시지를 읽을 때 Page Cache를 활용하므로 JVM에 모든 메모리를 할당하지 않고 운영체제가 사용할 메모리를 확보해야 합니다.

#### Disk: 30 GB

Kafka는 메시지를 메모리에서만 전달하지 않고 Disk에 기록합니다.

현재 환경에서는 다음 조건을 고려했습니다.

- 개발 및 테스트 목적
- 주문·취소·체결 이벤트 저장
- 메시지 보존 기간 7일
- 단일 Broker 구성
- 전체 Datastore를 다른 VM과 공동 사용

전체 Datastore는 계정계 Active/Standby, 체결 엔진 Active/Standby 및 Kafka VM이 공동으로 사용하기 때문에 Kafka VM에는 30 GB를 할당했습니다.

운영 환경에서는 다음 값을 기준으로 저장 용량을 다시 계산해야 합니다.

```text
평균 메시지 크기
초당 메시지 수
메시지 보존 기간
Replication Factor
로그 및 시스템 여유 공간
```

#### 고정 IP: `10.4.10.33`

Producer와 Consumer는 다음 주소를 통해 Kafka Broker에 접근합니다.

```yaml
spring:
  kafka:
    bootstrap-servers: 10.4.10.33:9092
```

Kafka VM의 IP가 변경되면 모든 Client의 연결이 중단될 수 있으므로 고정 IP를 사용합니다.

#### VLAN 10

```text
계정계 서버 : 10.4.10.11
체결 엔진   : 10.4.10.22
Kafka VM    : 10.4.10.33
```

Kafka와 주요 온프레미스 시스템을 동일 VLAN에 배치하여 내부 통신 구조를 단순화했습니다.

---

### 2.3 Docker Compose 구성 요소

```text
Docker Compose
├── kafka
│   ├── Kafka Broker
│   └── KRaft Controller
│
└── kafka-ui
    ├── Topic 조회
    ├── Message 조회
    ├── Consumer Group 조회
    └── Consumer Lag 조회
```

Kafka Broker와 Controller는 하나의 Container에서 실행하며, Kafka UI는 별도의 Container로 실행합니다.

Kafka 데이터는 Docker Named Volume에 저장합니다.

```text
Docker Named Volume: kafka-data
        ↓
Container 내부 경로: /var/lib/kafka/data
```

따라서 `/var/lib/kafka/data`는 호스트의 고정 경로가 아니라 Kafka Container 내부의 Mount 경로입니다.

---

## 3. 선택 이유 및 대안 비교

### 3.1 Kafka 선택 이유

#### 비동기 처리

주문 서비스는 체결 엔진의 처리가 완료될 때까지 동기적으로 기다리지 않고 주문 접수 처리를 진행할 수 있습니다.

```text
동기 방식

order-service
    ↓ HTTP 요청
체결 엔진
    ↓ 처리 완료까지 대기
order-service 응답
```

```text
Kafka 비동기 방식

order-service
    ↓ order.request 발행
Kafka
    ↓
체결 엔진이 독립적으로 처리
```

#### 장애 격리

체결 엔진이나 `trade-sync-worker`가 일시적으로 중단되더라도 Kafka가 이벤트를 보관할 수 있습니다.

Consumer가 복구되면 마지막으로 처리한 Offset 이후부터 메시지를 이어서 처리할 수 있습니다.

#### 처리량 확장

Topic을 여러 Partition으로 구성하고 Consumer Group에 Consumer를 추가하면 메시지를 병렬로 처리할 수 있습니다.

#### 메시지 순서 관리

주문 ID를 Message Key로 사용하면 동일한 주문 ID를 가진 이벤트가 같은 Partition으로 전달됩니다.

```text
orderId=1001
├── order.request
├── order.cancel
└── execution.confirmed
```

동일한 Key가 같은 Partition에 들어가면 Partition 내부 순서를 유지할 수 있습니다.

#### 이벤트 재처리

Consumer Offset을 기준으로 처리 위치를 관리하기 때문에 필요한 경우 이전 Offset부터 메시지를 다시 읽을 수 있습니다.

---

### 3.2 Docker 기반 설치를 선택한 이유

Kafka를 Rocky Linux에 직접 설치하지 않고 Docker Compose로 실행합니다.

| 구분 | 직접 설치 | Docker Compose |
|---|---|---|
| Java 설치 | 별도 설치 필요 | Kafka Image에 포함 |
| Kafka 설치 | 다운로드 및 압축 해제 | Image Pull |
| 설정 관리 | 서버 파일 직접 관리 | Compose YAML 관리 |
| 버전 변경 | 파일 수동 교체 | Image Tag 변경 |
| 재설치 | 파일 및 서비스 정리 필요 | Container 재생성 |
| 데이터 유지 | 경로 직접 관리 | Volume 사용 |
| 환경 재현 | 서버 상태에 영향을 받음 | Compose 파일로 재현 |
| 팀 공유 | 설치 절차 공유 필요 | YAML 공유 가능 |

Docker Compose를 사용하면 Kafka와 Kafka UI의 버전, Port, 환경변수 및 Volume 구성을 하나의 파일로 관리할 수 있습니다.

---

### 3.3 KRaft를 선택한 이유

기존 Kafka는 Cluster Metadata를 관리하기 위해 ZooKeeper가 필요했습니다.

```text
기존 방식

Kafka Broker
    ↕
ZooKeeper
```

KRaft에서는 Kafka가 자체 Controller를 통해 Metadata를 관리합니다.

```text
KRaft 방식

Kafka Broker
+
Kafka Controller
```

KRaft를 선택한 이유는 다음과 같습니다.

- ZooKeeper 별도 설치 불필요
- 운영 구성 요소 감소
- Docker Compose 구성 단순화
- Kafka Broker와 Metadata 관리 통합
- 개발 및 테스트 환경에 적합

---

### 3.4 단일 Broker 구성 이유

현재 Kafka는 Broker 1대로 구성합니다.

```text
Broker 수            : 1
Controller 수        : 1
Replication Factor   : 1
```

단일 Broker를 선택한 이유는 다음과 같습니다.

- 개발 및 테스트 환경
- 제한된 VM 및 Datastore 자원
- 초기 주문·체결 흐름 검증 목적
- 고가용성보다 구성 단순성이 우선

단일 Broker 환경에서는 Kafka VM 장애 시 전체 메시지 처리가 중단됩니다.

운영 환경에서는 Broker 3대 이상과 KRaft Controller Quorum을 구성하고 Replication Factor를 3으로 설정하는 방안을 검토해야 합니다.

---

### 3.5 대안 비교

#### RabbitMQ와 비교

| 구분 | Kafka | RabbitMQ |
|---|---|---|
| 주요 목적 | 이벤트 스트리밍과 로그 | 메시지 Queue |
| 메시지 보존 | 설정 기간 동안 보존 | 소비 후 제거 중심 |
| 재처리 | Offset 기반 | 별도 설계 필요 |
| 대용량 처리 | 높은 처리량에 적합 | 일반 메시징에 적합 |
| 순서 보장 | Partition 단위 | Queue 단위 |
| 프로젝트 적합성 | 주문·체결 이벤트 이력에 적합 | 단순 작업 전달에 적합 |

MaeSoonGan은 주문 및 체결 이벤트를 일정 기간 보관하고 장애 발생 시 재처리할 가능성이 있으므로 Kafka를 선택했습니다.

#### HTTP 직접 통신과 비교

| 구분 | HTTP 직접 호출 | Kafka |
|---|---|---|
| 연결 방식 | 동기 | 비동기 |
| 상대 서비스 장애 | 호출 서비스까지 전파 가능 | 메시지 보관 가능 |
| 재처리 | 별도 구현 필요 | Offset 기반 |
| 서비스 결합도 | 높음 | 낮음 |
| 구성 복잡도 | 낮음 | 상대적으로 높음 |

---

## 4. 설치 및 주요 설정

### 4.1 VM 생성

vCenter에서 다음 사양으로 VM을 생성합니다.

```text
VM 이름 : kafka-vm
OS      : Rocky Linux 9
CPU     : 2 Core
Memory  : 4 GB
Disk    : 30 GB
Network : VLAN 10
IP      : 10.4.10.33
```

VM 생성 시 Rocky Linux ISO를 CD/DVD Drive에 연결하고 전원 시작 시 연결되도록 설정합니다.

---

### 4.2 Rocky Linux 네트워크 설정

```text
Host Name : kafka-vm
IP Address: 10.4.10.33
Netmask   : 255.255.255.0
Gateway   : 10.4.10.1
DNS       : 내부 DNS 또는 접근 가능한 DNS
```

설치 후 IP와 Routing Table을 확인합니다.

```bash
ip addr show
ip route show
```

정상 결과에는 다음 정보가 포함되어야 합니다.

```text
inet 10.4.10.33/24
default via 10.4.10.1
```

동일 VLAN의 서버와 통신을 확인합니다.

```bash
ping 10.4.10.1
ping 10.4.10.11
ping 10.4.10.22
```

---

### 4.3 Docker 설치

Docker Repository를 추가합니다.

```bash
sudo dnf update -y

sudo dnf config-manager --add-repo \
  https://download.docker.com/linux/rhel/docker-ce.repo
```

Docker와 Docker Compose Plugin을 설치합니다.

```bash
sudo dnf install -y \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-compose-plugin
```

Docker를 시작하고 자동 시작을 설정합니다.

```bash
sudo systemctl enable --now docker
```

현재 사용자를 Docker Group에 추가합니다.

```bash
sudo usermod -aG docker "$USER"
newgrp docker
```

설치 결과를 확인합니다.

```bash
docker --version
docker compose version
sudo systemctl status docker
```

---

### 4.4 Kafka 작업 디렉터리 생성

```bash
sudo mkdir -p /opt/kafka
sudo chown -R "$USER":"$USER" /opt/kafka

cd /opt/kafka
```

`/opt`는 일반 사용자에게 기본 쓰기 권한이 없으므로 `sudo`를 이용해 디렉터리를 생성합니다.

---

### 4.5 Docker Compose 작성

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: kafka
    restart: always

    ports:
      - "9092:9092"

    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller

      KAFKA_CONTROLLER_QUORUM_VOTERS: >-
        1@kafka:9093

      KAFKA_LISTENERS: >-
        PLAINTEXT://0.0.0.0:9092,
        CONTROLLER://0.0.0.0:9093

      KAFKA_ADVERTISED_LISTENERS: >-
        PLAINTEXT://10.4.10.33:9092

      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >-
        PLAINTEXT:PLAINTEXT,
        CONTROLLER:PLAINTEXT

      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

      KAFKA_NUM_PARTITIONS: 3
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_SEGMENT_BYTES: 1073741824

      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"

    volumes:
      - kafka-data:/var/lib/kafka/data

  kafka-ui:
    image: provectuslabs/kafka-ui:v0.7.2
    container_name: kafka-ui
    restart: always

    ports:
      - "8989:8080"

    environment:
      KAFKA_CLUSTERS_0_NAME: maesoongan-kafka
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092

    depends_on:
      - kafka

volumes:
  kafka-data:
```

Kafka UI는 동일한 Docker Compose Network에서 Kafka Container에 접근하므로 `kafka:9092`를 Bootstrap Server로 사용합니다.

KRaft Controller Port `9093`은 Kafka 내부 Controller 통신에 사용하므로 Host에 공개하지 않습니다.

현재 구성은 내부 개발·테스트 네트워크에서 사용하는 PLAINTEXT 구성입니다.

```text
Kafka Client 인증 없음
Network 전송 암호화 없음
```

운영 환경에서는 SASL/SCRAM 또는 SASL/OAUTHBEARER와 TLS 적용을 검토해야 합니다.

---

### 4.6 주요 설정 설명

#### Kafka Node ID

```yaml
KAFKA_NODE_ID: 1
```

Kafka Cluster 내부에서 Broker와 Controller를 식별하는 고유 ID입니다.

#### Broker 및 Controller 역할

```yaml
KAFKA_PROCESS_ROLES: broker,controller
```

하나의 Kafka Container가 Broker와 KRaft Controller 역할을 함께 수행합니다.

#### Controller Quorum

```yaml
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

Controller Node ID와 내부 통신 주소를 지정합니다.

#### Listener

```yaml
KAFKA_LISTENERS: >-
  PLAINTEXT://0.0.0.0:9092,
  CONTROLLER://0.0.0.0:9093
```

Kafka Container가 실제로 요청을 수신하는 주소입니다.

#### Advertised Listener

```yaml
KAFKA_ADVERTISED_LISTENERS: >-
  PLAINTEXT://10.4.10.33:9092
```

Kafka가 외부 Producer와 Consumer에게 안내하는 Broker 주소입니다.

다른 VM과 EKS 서비스가 접근해야 하므로 `localhost` 또는 Docker Container 이름이 아니라 Kafka VM의 고정 IP를 지정합니다.

#### Replication Factor

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

현재 Broker가 1대이므로 Kafka 내부 Topic의 Replication Factor와 최소 ISR을 1로 설정합니다.

#### 기본 Partition

```yaml
KAFKA_NUM_PARTITIONS: 3
```

Topic 생성 시 Partition 수를 별도로 지정하지 않은 경우 적용되는 기본값입니다.

#### 메시지 보존 기간

```yaml
KAFKA_LOG_RETENTION_HOURS: 168
```

Kafka 메시지를 168시간, 즉 7일 동안 보관합니다.

---

### 4.7 Kafka 실행

```bash
cd /opt/kafka
docker compose up -d
```

Container 상태를 확인합니다.

```bash
docker compose ps
```

Kafka 로그를 확인합니다.

```bash
docker compose logs -f kafka
```

다음과 유사한 로그가 출력되면 정상적으로 시작된 것입니다.

```text
Kafka Server started
```

로그 확인을 종료할 때 `Ctrl + C`를 입력해도 Background Container는 계속 실행됩니다.

---

### 4.8 Topic 생성

#### 주문 요청 Topic

```bash
docker exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server kafka:9092 \
  --topic order.request \
  --partitions 3 \
  --replication-factor 1
```

#### 주문 취소 Topic

```bash
docker exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server kafka:9092 \
  --topic order.cancel \
  --partitions 3 \
  --replication-factor 1
```

#### 체결 결과 Topic

```bash
docker exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server kafka:9092 \
  --topic execution.confirmed \
  --partitions 3 \
  --replication-factor 1
```

#### 회원·계정계 명령 결과 Topic

```bash
docker exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server kafka:9092 \
  --topic member.command.result \
  --partitions 3 \
  --replication-factor 1
```

Topic 목록을 확인합니다.

```bash
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092
```

Topic 상세 정보를 확인합니다.

```bash
docker exec kafka kafka-topics \
  --describe \
  --bootstrap-server kafka:9092 \
  --topic order.request
```

---

### 4.9 방화벽 설정

Rocky Linux는 `firewalld`를 사용합니다.

Kafka Broker Port를 허용합니다.

```bash
sudo firewall-cmd \
  --permanent \
  --add-port=9092/tcp
```

Kafka UI Port를 허용합니다.

```bash
sudo firewall-cmd \
  --permanent \
  --add-port=8989/tcp
```

설정을 적용합니다.

```bash
sudo firewall-cmd --reload
```

허용된 Port를 확인합니다.

```bash
sudo firewall-cmd --list-ports
```

KRaft Controller Port인 `9093`은 외부 Client가 사용할 필요가 없으므로 Host 방화벽에 공개하지 않습니다.

운영 환경에서는 Source IP 또는 Source 대역을 제한하고 Kafka UI도 관리자 네트워크에서만 접근하도록 구성해야 합니다.

---

### 4.10 Kafka UI 접속

브라우저에서 다음 주소로 접속합니다.

```text
http://10.4.10.33:8989
```

Kafka UI에서는 다음 항목을 확인할 수 있습니다.

- Broker 상태
- Topic 목록
- Partition 상태
- Topic 메시지
- Consumer Group
- Consumer Lag

현재 Kafka UI에는 별도 인증이 적용되지 않았으므로 내부 개발 네트워크에서만 사용해야 합니다.

---

### 4.11 Spring Boot 연동

`application-onprem.yml` 또는 해당 환경 설정 파일에 Kafka Broker 주소를 추가합니다.

```yaml
spring:
  kafka:
    bootstrap-servers: 10.4.10.33:9092

    producer:
      key-serializer: >-
        org.apache.kafka.common.serialization.StringSerializer
      value-serializer: >-
        org.springframework.kafka.support.serializer.JsonSerializer

    consumer:
      group-id: maesoongan-group
      key-deserializer: >-
        org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: >-
        org.springframework.kafka.support.serializer.JsonDeserializer
      auto-offset-reset: earliest

      properties:
        spring.json.trusted.packages: com.maesoongan
```

`build.gradle`에는 Spring Kafka 의존성을 추가합니다.

```gradle
implementation 'org.springframework.kafka:spring-kafka'
```

Topic 이름은 코드 또는 환경변수에서 다음 값과 일치하도록 구성합니다.

```yaml
kafka:
  topic:
    order-request: order.request
    order-cancel: order.cancel
    execution-confirmed: execution.confirmed
    member-command-result: member.command.result
```

실제 프로젝트의 Property 구조가 다르면 코드에서 사용하는 설정 Key에 맞게 수정합니다.

---

## 5. 동작 흐름 및 트러블슈팅

### 5.1 주문 처리 동작 흐름

```text
1. 사용자가 주문 요청
        ↓
2. order-service가 주문 유효성 검증
        ↓
3. 주문 가능 금액 또는 보유 수량 예약
        ↓
4. 주문을 PENDING 상태로 저장
        ↓
5. order.request Topic에 주문 이벤트 발행
        ↓
6. 체결 엔진이 주문 이벤트 수신
        ↓
7. Order Book 등록 및 매수·매도 주문 매칭
        ↓
8. execution.confirmed Topic에 체결 결과 발행
        ↓
9. trade-sync-worker가 체결 결과 수신
        ↓
10. 주문·체결·포트폴리오 상태 동기화
```

---

### 5.2 주문 취소 흐름

```text
1. 사용자가 주문 취소 요청
        ↓
2. order-service가 취소 가능 상태 검증
        ↓
3. order.cancel Topic에 취소 이벤트 발행
        ↓
4. 체결 엔진이 취소 이벤트 수신
        ↓
5. Order Book에서 미체결 주문 취소
        ↓
6. 처리 결과 이벤트 발행
        ↓
7. trade-sync-worker가 결과 수신
        ↓
8. 주문 상태와 예약 금액·수량 동기화
```

---

### 5.3 계정계 명령 결과 흐름

```text
계정계 또는 회원 서비스
        ↓
회원·잔고 관련 명령 처리
        ↓
member.command.result 발행
        ↓
trade-sync-worker 수신
        ↓
관련 데이터와 상태 동기화
```

실제 Producer는 계정계 및 회원 서비스 구현 구조에 따라 문서에 구체적으로 명시합니다.

---

### 5.4 Producer 동작 흐름

```text
order-service
    ↓
KafkaTemplate.send()
    ↓
Kafka Broker 10.4.10.33:9092
    ↓
order.request 또는 order.cancel
    ↓
Message Key를 기준으로 Partition 선택
```

주문 ID를 Message Key로 사용하면 동일 주문과 관련된 이벤트를 같은 Partition으로 전달할 수 있습니다.

---

### 5.5 Consumer 동작 흐름

```text
execution.confirmed
    ↓
trade-sync-worker Consumer Group
    ↓
trade-sync-worker
    ↓
DB 동기화
    ↓
Offset Commit
```

메시지 처리와 DB 반영이 완료된 후 Offset을 Commit하도록 구성해야 메시지 유실 가능성을 줄일 수 있습니다.

중복 메시지 수신 가능성을 고려해 Consumer 로직은 멱등성을 갖도록 설계하는 것이 좋습니다.

---

### 5.6 VM 네트워크 통신 확인

Kafka VM의 IP를 확인합니다.

```bash
ip addr show
```

Gateway를 확인합니다.

```bash
ip route show
```

같은 VLAN 서버와 통신을 확인합니다.

```bash
ping 10.4.10.1
ping 10.4.10.11
ping 10.4.10.22
```

계정계 서버 또는 체결 엔진에서 Kafka Port 연결을 확인합니다.

```bash
nc -zv 10.4.10.33 9092
```

---

### 5.7 `/opt/kafka` 권한 오류

발생 오류:

```text
mkdir: /opt/kafka 디렉터리를 만들 수 없습니다: 허가 거부
```

원인:

```text
/opt 디렉터리에 일반 사용자 쓰기 권한이 없음
```

해결:

```bash
sudo mkdir -p /opt/kafka
sudo chown -R "$USER":"$USER" /opt/kafka
```

---

### 5.8 `nano` 명령어 없음

발생 오류:

```text
nano: command not found
```

Rocky Linux Minimal 환경에는 `nano`가 기본으로 설치되지 않을 수 있습니다.

```bash
sudo dnf install -y nano
```

또는 `vi`를 사용합니다.

```bash
vi docker-compose.yml
```

---

### 5.9 Docker Compose 파일 저장 권한 오류

`/opt/kafka`의 소유자가 `root`이면 일반 사용자가 파일을 저장할 수 없습니다.

```bash
sudo chown -R "$USER":"$USER" /opt/kafka
```

또는 `sudo tee`를 이용해 파일을 작성합니다.

---

### 5.10 Kafka Container 실행 실패

상태를 확인합니다.

```bash
docker compose ps -a
```

Kafka 로그를 확인합니다.

```bash
docker compose logs kafka
```

주요 확인 항목은 다음과 같습니다.

- `CLUSTER_ID` 누락
- Listener 설정 오류
- Volume 권한 오류
- Port 중복
- YAML 들여쓰기 오류
- Docker Daemon 중지
- 기존 Volume의 Cluster Metadata 충돌

Docker 상태를 확인합니다.

```bash
sudo systemctl status docker
```

---

### 5.11 Kafka 연결 실패

대표 오류:

```text
Connection to node could not be established
Broker may not be available
```

확인 순서:

```bash
docker compose ps
sudo firewall-cmd --list-ports
ss -lntp | grep 9092
nc -zv 10.4.10.33 9092
```

주요 원인은 다음과 같습니다.

- Kafka Container 중지
- Port `9092` 미허용
- VLAN 또는 Routing 문제
- `advertised.listeners` 주소 오류
- Spring Boot `bootstrap-servers` 오류
- EKS와 온프레미스 사이 VPN 또는 Route 문제

---

### 5.12 `advertised.listeners` 오류

Client는 처음에는 `bootstrap-servers` 주소로 Kafka에 접근하지만, 이후 Broker가 반환한 `advertised.listeners` 주소로 연결합니다.

잘못된 설정:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

이 경우 다른 VM 또는 EKS Pod는 자기 자신의 `localhost`에 접근하려고 합니다.

올바른 설정:

```yaml
KAFKA_ADVERTISED_LISTENERS: >-
  PLAINTEXT://10.4.10.33:9092
```

---

### 5.13 Topic 이름 불일치

Producer가 발행하는 Topic과 Consumer가 구독하는 Topic 이름이 다르면 메시지가 전달되지 않습니다.

예시:

```text
Producer: order.request
Consumer: order-topic
```

Topic 이름은 다음 위치에서 모두 일치해야 합니다.

- Kafka Topic 생성 명령
- Producer 코드
- Consumer 코드
- `application.yml`
- 환경변수
- 테스트 코드
- 운영 문서

현재 기준 Topic:

```text
order.request
order.cancel
execution.confirmed
member.command.result
```

---

### 5.14 Topic 생성 오류

이미 존재하는 Topic을 다시 생성하면 다음 오류가 발생할 수 있습니다.

```text
Topic already exists
```

`--if-not-exists` 옵션을 사용합니다.

```bash
docker exec kafka kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server kafka:9092 \
  --topic order.request \
  --partitions 3 \
  --replication-factor 1
```

---

### 5.15 Replication Factor 오류

단일 Broker 환경에서 Replication Factor를 2 이상으로 설정하면 오류가 발생합니다.

```text
Replication factor larger than available brokers
```

현재 설정:

```text
Broker 수           : 1
Replication Factor  : 1
```

운영 환경에서는 Broker 수를 늘린 후 Replication Factor를 3으로 조정합니다.

---

### 5.16 Kafka UI 접속 실패

```bash
docker compose ps
docker compose logs kafka-ui
sudo firewall-cmd --list-ports
ss -lntp | grep 8989
```

접속 주소:

```text
http://10.4.10.33:8989
```

Kafka UI가 Broker에 연결하지 못하면 다음 설정을 확인합니다.

```yaml
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
```

---

### 5.17 데이터 유지 확인

Kafka 데이터는 Docker Named Volume `kafka-data`에 저장됩니다.

```bash
docker volume ls
```

Compose 서비스만 중지하고 Container를 제거합니다.

```bash
docker compose down
```

다시 시작합니다.

```bash
docker compose up -d
```

Named Volume이 유지되므로 기존 Kafka 데이터와 Metadata도 유지됩니다.

다음 명령은 Volume까지 삭제하므로 주의해야 합니다.

```bash
docker compose down -v
```

`-v` 옵션을 사용하면 Kafka 메시지와 KRaft Metadata가 삭제될 수 있습니다.

---

### 5.18 운영 명령어

#### Container 상태

```bash
docker compose ps
```

#### 전체 로그

```bash
docker compose logs
```

#### Kafka 실시간 로그

```bash
docker compose logs -f kafka
```

#### Kafka 재시작

```bash
docker compose restart kafka
```

#### 전체 서비스 재시작

```bash
docker compose restart
```

#### Topic 목록

```bash
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092
```

#### Consumer Group 목록

```bash
docker exec kafka kafka-consumer-groups \
  --list \
  --bootstrap-server kafka:9092
```

#### Consumer Lag 확인

```bash
docker exec kafka kafka-consumer-groups \
  --describe \
  --bootstrap-server kafka:9092 \
  --group maesoongan-group
```

실제 서비스별 Consumer Group이 다르면 해당 Group ID를 사용합니다.

---

### 5.19 현재 구성의 한계

현재 구성은 개발 및 테스트 목적의 단일 Broker 환경입니다.

```text
Kafka Broker        1대
KRaft Controller    1대
Replication Factor  1
Security Protocol   PLAINTEXT
Kafka UI 인증       없음
```

따라서 다음 한계가 있습니다.

- Kafka VM 장애 시 전체 Kafka 중단
- Broker 데이터 복제 없음
- Controller 장애 복구 불가능
- Rolling Update 어려움
- Client 인증 없음
- 전송 구간 암호화 없음
- Kafka UI 접근 인증 없음

---

### 5.20 향후 개선 계획

- Kafka Broker 3대 구성
- KRaft Controller Quorum 구성
- Replication Factor 3 적용
- `min.insync.replicas` 설정
- SASL 인증 적용
- TLS 암호화 적용
- Kafka UI 접근 인증 및 IP 제한
- Producer Idempotence 적용
- Producer Retry 및 Acknowledgment 정책 정립
- Consumer 수동 Commit 검토
- Consumer 멱등성 강화
- Dead Letter Topic 구성
- Consumer Group 서비스별 분리
- Consumer Lag 모니터링
- Prometheus 및 Grafana 연동
- Disk 사용량 알림
- Topic Naming Convention 문서화
- Schema Registry 도입 검토
