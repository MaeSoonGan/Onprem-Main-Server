# 🗄️ MySQL Semi-sync 복제 구성

## 1. 개요 및 프로젝트 내 역할

주센터 DB Cluster는 MySQL 8.0 기반의 **2-Tier 복제** 구조로 구성됩니다. 주센터 내부는 Semi-sync 복제로 데이터 무손실을 보장하고, 주센터 → DR센터 구간은 Async 복제로 WAN 지연 영향을 피합니다.

| 구간 | 복제 방식 | 이유 |
| --- | --- | --- |
| 주센터 내부 (Primary ↔ Replica-1) | Semi-sync | 데이터 무손실(RPO ≈ 0) |
| 주센터 → DR (Primary → Replica-2) | Async | WAN ACK 지연을 피해 쓰기 성능 보호 |

본 문서는 이 중 **주센터 내부 Semi-sync 복제** 구성에 집중합니다. DR Async 복제·페일오버 자동화는 각각 DR 레포 및 [Orchestrator 문서](./orchestrator.md)에서 다룹니다.

<br />

## 2. 구성 환경

| 구분 | 내용 |
| --- | --- |
| DBMS | MySQL 8.0 |
| OS | Rocky Linux 9 (AppStream 패키지) |
| 복제 식별 | GTID 기반 복제 |
| 토폴로지 | Primary + Replica-1 (Semi-sync), Replica-2 (Async, DR) |

GTID 기반을 선택한 이유는, 페일오버로 Primary가 바뀌어도 각 트랜잭션이 전역 고유 ID로 식별되어 복제 재구성(`CHANGE REPLICATION SOURCE ... FOR CHANNEL`)이 안전하고 단순해지기 때문입니다.

<br />

## 3. 선택 이유 및 대안 비교

### 왜 Semi-sync인가 (Async / 완전 동기 대비)

- **Async 대비:** Async는 Primary가 커밋 후 즉시 응답하므로 Primary 장애 시 미전송 트랜잭션이 유실될 수 있습니다. Semi-sync는 최소 1개 Replica가 binlog를 수신했다는 ACK를 받은 뒤 커밋을 완료하므로, 주센터 내부에서 Primary가 죽어도 Replica-1에 데이터가 보장됩니다.
- **완전 동기(Group Replication) 대비:** 완전 동기/그룹 복제는 정합성은 강하지만 구성 복잡도와 쓰기 지연이 커집니다. 단일 사이트 HA 목적에는 Semi-sync가 성능과 무손실의 균형점입니다.

### 왜 DR 구간은 Async인가

주센터 → DR은 WAN(VLAN 30 경유)을 타므로, 이 구간까지 Semi-sync를 걸면 모든 쓰기가 WAN 왕복 지연을 기다려야 합니다. 실시간 체결 처리에 부적합하므로 DR 구간만 Async로 분리했습니다. 이는 한국 금융권의 실제 2-Tier 복제 패턴과 일치합니다.

<br />

## 4. 주요 설정

### 4-1. Semi-sync 플러그인 및 변수

MySQL 8.0.26 이상에서 **semi-sync 관련 변수명이 변경**되었습니다. (`rpl_semi_sync_master_*` → `rpl_semi_sync_source_*`, `rpl_semi_sync_slave_*` → `rpl_semi_sync_replica_*`)

```ini
# Primary (my.cnf)
rpl_semi_sync_source_enabled = 1
rpl_semi_sync_source_timeout = 1000   # ACK 대기 타임아웃(ms)

# Replica (my.cnf)
rpl_semi_sync_replica_enabled = 1
```

> ⚠️ **TRAP — Orchestrator 호환성**
> Orchestrator v3.2.6은 위 변수명 변경을 따라가지 못해, 기본 동작에서 semi-sync 관련 쿼리가 실패합니다. semi-sync 관리를 Orchestrator에 맡기지 않고 **my.cnf와 PostFailover 훅에 위임**하기 위해 Orchestrator 설정에서 `DetectSemiSyncEnforcedQuery: ""` 와 `EnforceSemiSyncReplicas: false` 를 지정합니다. (상세는 Orchestrator 문서 참조)

### 4-2. read_only 강제 (split-brain 방지)

```ini
# 모든 노드 my.cnf 공통
read_only = ON
super_read_only = ON
```

> ⚠️ **TRAP — 비대칭 설정 금지**
> 일부 노드만 `read_only=ON`을 빼는 비대칭 설정은 split-brain 위험을 만듭니다. **모든 노드를 read_only=ON으로 통일**하고, 승격은 오직 Orchestrator의 PostFailover 훅이 수행하도록 합니다.

### 4-3. 복제 계정 동기화

복제용 계정(repl 계정)이 한 노드에만 존재하면, 페일오버 후 새 Primary에서 복제가 붙지 못합니다.

> ⚠️ **TRAP — 계정 누락**
> 복제 계정을 복제 시작 전에 생성하면, 해당 계정이 binlog로 전파되지 않아 한쪽 노드에만 존재할 수 있습니다. **모든 노드에 복제 계정이 동일하게 존재**하는지 확인합니다.

<br />

## 5. 동작 흐름 및 트러블슈팅

### 정상 동작 흐름

1. Primary가 쓰기 트랜잭션을 처리하고 binlog에 기록합니다.
2. Replica-1이 binlog를 수신하면 ACK를 반환하고, Primary는 ACK 수신 후 커밋을 완료합니다(Semi-sync).
3. DR의 Replica-2는 같은 Primary로부터 Async로 복제를 이어받습니다.
4. ProxySQL이 쓰기는 Primary로, 읽기는 Replica로 라우팅합니다. (상세: [ProxySQL 문서](./proxysql.md))

### 트러블슈팅

| 증상 | 원인 | 조치 |
| --- | --- | --- |
| Orchestrator에서 semi-sync 관련 쿼리 오류 | MySQL 8.0.26+ 변수명 변경 미대응 | `DetectSemiSyncEnforcedQuery: ""`, `EnforceSemiSyncReplicas: false` 적용 후 semi-sync 관리는 my.cnf/훅에 위임 |
| 페일오버 후 복제가 붙지 않음 | 복제 계정이 일부 노드에만 존재 | 모든 노드에 동일 복제 계정 생성 확인 |
| 재부팅된 노드가 stale read 반환 | read_only=ON이지만 복제 미구성 상태로 reader 풀에 즉시 편입 | 노드 재투입 절차(`db-rejoin.sh`)로 복제 catch-up 후 ONLINE 처리 |
| Primary 장애 시 데이터 유실 우려 | Async 구간 특성 | 주센터 내부는 Semi-sync로 무손실 보장, DR 구간만 Async로 분리됨을 확인 |
