# 🎯 Orchestrator 자동 페일오버

## 1. 개요 및 프로젝트 내 역할

Orchestrator는 MySQL 복제 토폴로지를 모니터링하고, Primary 장애를 감지하면 Replica를 자동으로 Primary로 승격시키는 페일오버 관리 도구입니다. 주센터 내부 장애와 주센터 전체 장애(→ DR 승격) 모두를 자동 페일오버로 처리하여, 무인 상태에서도 서비스가 이어지도록 합니다.

| 항목 | 내용 |
| --- | --- |
| 버전 | Orchestrator v3.2.6 |
| 백엔드 | SQLite |
| 관리 대상 | 주센터 Primary, 주센터 Replica-1, DR Replica-2 |

<br />

## 2. 배치 — 판정자의 SPOF 제거

Orchestrator(와 ProxySQL)는 처음에 주센터 Kafka VM에 함께 두었으나, **독립된 모니터링 서버 VM(Ubuntu 24.04)으로 이전**했습니다.

> ⚠️ **설계 판단 — 판정자를 주센터 밖에 둔다**
> 페일오버를 판정하는 Orchestrator가 주센터 안에 있으면, "주센터 전원 차단" 시 판정자까지 함께 죽어 DR 승격을 내릴 주체가 사라집니다. 판정자를 주센터·DR과 분리된 모니터링 서버에 두어, 주센터 전원과 무관하게 살아남아 DR 승격을 자율적으로 결정하게 했습니다.

<br />

## 3. 승격 정책 — register-candidate

주센터 노드는 우선 승격, DR은 최후의 수단으로만 승격하도록 승격 규칙을 부여합니다.

```bash
# 주센터 Primary / Replica-1 → prefer (우선 승격 후보)
orchestrator -c register-candidate -i <primary-host>:3306    --promotion-rule prefer
orchestrator -c register-candidate -i <replica1-host>:3306   --promotion-rule prefer

# DR Replica-2 → prefer_not (다른 후보가 모두 죽었을 때만 승격)
orchestrator -c register-candidate -i <dr-replica-host>:3306 --promotion-rule prefer_not
```

규칙의 의미:

- `prefer` (주센터 Primary/Replica-1): 주센터 Primary 장애 시 이 둘 중에서 승격
- `prefer_not` (DR Replica-2): 주센터 노드가 모두 죽었을 때만 DR 승격

이로써 **주센터 내부 장애는 주센터 안에서, 주센터 전체 장애는 DR로** 라는 의도된 동작이 보장됩니다.

> ⚠️ **TRAP — register-candidate는 만료된다**
> `register-candidate`는 일회성 등록이며 일정 시간(기본 `CandidateInstanceExpireMinutes` ≈ 1시간) 후 만료됩니다. 등록 직후의 테스트에는 문제없지만, 한참 뒤 페일오버가 발생하면 규칙이 풀려 있을 수 있습니다. 영구 적용하려면 각 DB가 자신의 승격 규칙을 테이블에 두고 Orchestrator가 `DetectPromotionRuleQuery`로 읽게 합니다.

<br />

## 4. 주요 설정

### 4-1. Semi-sync 관리 위임 (8.0.26+ 호환)

> ⚠️ **TRAP — Orchestrator v3.2.6과 MySQL 8.0.26+ 변수명 변경**
> MySQL 8.0.26부터 semi-sync 변수명이 변경(`rpl_semi_sync_master_*` → `rpl_semi_sync_source_*`)되었는데, Orchestrator v3.2.6은 이를 따라가지 못해 semi-sync 강제 관련 쿼리가 실패합니다. semi-sync 관리를 Orchestrator에 맡기지 않고 **my.cnf와 PostFailover 훅에 위임**하기 위해 다음을 설정합니다.

```json
{
  "DetectSemiSyncEnforcedQuery": "",
  "EnforceSemiSyncReplicas": false
}
```

### 4-2. PostFailover 훅

페일오버 직후 자동으로 실행되는 후처리 스크립트를 등록합니다. 본 프로젝트의 훅은 다음을 수행합니다.

- 승격된 신규 Primary에서 semi-sync(source) 활성화 복원
- 신규 Primary를 ProxySQL에서 `ONLINE` 상태로 보장 (DR 승격 시 reader 대기 상태였던 노드를 트래픽 수신 가능하게 전환)
- 채널계 정합성 동기화 API 호출 트리거 (원장 DB 기준 RDS·Redis 재구성)

```json
{
  "PostFailoverProcesses": [
    "/usr/local/bin/orch-post-failover.sh {failureType} {failureCluster} {successorHost} >> /var/log/orch-hook.log 2>&1"
  ]
}
```

> "신규 Primary는 무조건 ProxySQL ONLINE 보장"을 일반 규칙으로 두면, 주센터 노드 페일오버(이미 ONLINE)에는 무해하고 DR 승격 시에만 실제 전환이 일어나므로 안전합니다.

<br />

## 5. 동작 흐름 및 트러블슈팅

### 페일오버 시나리오

**시나리오 A — 주센터 Primary만 장애 (주센터는 살아있음)**
Orchestrator가 장애를 감지하고 `prefer` 후보(Replica-1)를 Primary로 승격 → ProxySQL이 새 Primary를 인식 → DR Replica-2는 새 Primary에서 복제를 이어받음. RTO 수 초.

**시나리오 B — 주센터 전체 장애 (전원 차단)**
주센터 Primary/Replica-1이 모두 불통 → 모니터링 서버의 Orchestrator가 이를 감지하고 `prefer_not`인 DR Replica-2를 Primary로 승격 → PostFailover 훅이 DR을 ProxySQL ONLINE으로 전환하고 정합성 동기화를 트리거 → Cold 대기 앱 VM 기동. Async 복제 지연만큼의 RPO 발생.

### 트러블슈팅

| 증상 | 원인 | 조치 |
| --- | --- | --- |
| semi-sync 관련 쿼리 실패 | v3.2.6 + 8.0.26+ 변수명 불일치 | `DetectSemiSyncEnforcedQuery: ""`, `EnforceSemiSyncReplicas: false` 적용, semi-sync는 my.cnf/훅으로 관리 |
| 한참 뒤 페일오버에서 DR이 우선 승격됨 | register-candidate 규칙 만료 | `DetectPromotionRuleQuery`로 승격 규칙 영구화 |
| 페일오버는 됐는데 신 Primary로 트래픽이 안 감 | ProxySQL 상태가 OFFLINE/미인식 | PostFailover 훅에서 신 Primary ONLINE 보장 로직 확인 |
| 주센터 내부 장애인데 DR로 승격됨 | 승격 규칙(prefer/prefer_not) 미적용 또는 만료 | 승격 규칙 재등록 및 토폴로지에서 규칙 표시 확인 |
