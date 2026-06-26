# 🔀 ProxySQL 라우팅 구성

## 1. 개요 및 프로젝트 내 역할

ProxySQL은 애플리케이션과 MySQL 사이에 위치하는 프록시로, 읽기/쓰기 쿼리를 적절한 노드로 라우팅합니다. 애플리케이션은 개별 DB 주소가 아닌 ProxySQL 한 곳(6033 포트)에만 접속하므로, 페일오버로 토폴로지가 바뀌어도 애플리케이션 설정 변경 없이 트래픽이 새 Primary로 흐릅니다.

| 항목 | 내용 |
| --- | --- |
| 버전 | ProxySQL 3.0.8 |
| 접속 포트 | 6033 (애플리케이션), 6032 (admin) |
| Writer Hostgroup | HG 10 |
| Reader Hostgroup | HG 20 |

<br />

## 2. 선택 이유

- **애플리케이션 무변경 페일오버:** 앱은 ProxySQL만 바라보므로, Primary가 바뀌어도 앱의 DB 접속 정보를 고칠 필요가 없습니다.
- **읽기 부하 분산:** SELECT를 Replica로 분산하여 Primary의 부하를 낮춥니다.
- **Orchestrator와의 연계:** read_only 상태를 기준으로 writer/reader를 자동 분류하므로, Orchestrator의 승격 결과가 ProxySQL 라우팅에 자연스럽게 반영됩니다.

<br />

## 3. 주요 설정

### 3-1. Replication Hostgroup (read_only 자동 분류)

`mysql_replication_hostgroups`를 설정하면, ProxySQL이 각 백엔드의 `read_only` 값을 보고 writer(HG10)/reader(HG20)로 자동 분류합니다.

```sql
-- HG10=writer, HG20=reader
INSERT INTO mysql_replication_hostgroups (writer_hostgroup, reader_hostgroup)
VALUES (10, 20);
```

> read_only=0인 노드(현재 Primary)는 HG10에 배치되고, read_only=1인 노드(Replica)는 HG20에 배치됩니다. Primary가 HG10과 HG20에 동시에 보이는 것은 정상이며, Primary도 읽기 쿼리를 받을 수 있음을 의미합니다.

### 3-2. Monitor 계정

ProxySQL이 백엔드의 read_only·헬스를 체크할 모니터 계정입니다. 백엔드 DB와 ProxySQL 양쪽에 설정합니다.

```sql
-- 현재 Primary DB에서 (복제로 전파)
CREATE USER IF NOT EXISTS 'proxymonitor'@'<proxysql-host>'
  IDENTIFIED WITH mysql_native_password BY '<password>';
GRANT USAGE, REPLICATION CLIENT ON *.* TO 'proxymonitor'@'<proxysql-host>';
FLUSH PRIVILEGES;
```

```sql
-- ProxySQL admin(6032)에서
UPDATE global_variables SET variable_value='proxymonitor' WHERE variable_name='mysql-monitor_username';
UPDATE global_variables SET variable_value='<password>'    WHERE variable_name='mysql-monitor_password';
LOAD MYSQL VARIABLES TO RUNTIME;
SAVE MYSQL VARIABLES TO DISK;
```

### 3-3. 애플리케이션 접속 계정

앱이 6033으로 접속할 계정으로, 백엔드 DB와 ProxySQL 양쪽 모두 등록합니다.

```sql
-- ProxySQL admin(6032)에서
INSERT INTO mysql_users (username, password, default_hostgroup, active)
VALUES ('appuser', '<password>', 10, 1);
LOAD MYSQL USERS TO RUNTIME;
SAVE MYSQL USERS TO DISK;
```

> `default_hostgroup=10`은 규칙에 매칭되지 않는 쿼리(INSERT/UPDATE/DELETE 등)를 기본적으로 writer로 보냅니다. 인증 매끄러움을 위해 `mysql_native_password`를 사용합니다.

### 3-4. 읽기/쓰기 분리 (Query Rules)

```sql
INSERT INTO mysql_query_rules (rule_id, active, match_digest, destination_hostgroup, apply) VALUES
  (100, 1, '^SELECT.*FOR UPDATE$', 10, 1),   -- 잠금 읽기 → writer
  (200, 1, '^SELECT',              20, 1);   -- 그 외 SELECT → reader
LOAD MYSQL QUERY RULES TO RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;
```

- rule 100: `SELECT ... FOR UPDATE`는 쓰기 트랜잭션의 일부이므로 writer(HG10)로
- rule 200: 그 외 모든 `SELECT`는 reader(HG20)로
- INSERT/UPDATE/DELETE는 매칭 규칙이 없어 `default_hostgroup=10`(writer)로

> ⚠️ **TRAP — 규칙 순서**
> `apply=1`은 매칭 시 이후 규칙 평가를 중단합니다. 따라서 rule 100(FOR UPDATE)을 rule 200(일반 SELECT)보다 **먼저** 두어야, 잠금 읽기가 일반 SELECT 규칙에 먼저 걸려 reader로 새는 것을 막을 수 있습니다.

### 3-5. 복제 지연 방어 (max_replication_lag)

```sql
UPDATE mysql_servers SET max_replication_lag = 10 WHERE hostgroup_id = 20;
LOAD MYSQL SERVERS TO RUNTIME;
SAVE MYSQL SERVERS TO DISK;
```

복제 지연이 임계치(예: 10초)를 넘는 Replica는 reader 풀에서 일시 제외하여 stale read를 줄입니다.

<br />

## 4. Stale Replica 방어 — read_only만으로는 부족

> ⚠️ **TRAP — 재기동 노드의 stale read**
> 죽었던 DB 노드가 재기동되면 `read_only=ON` 상태로 뜨는데, **이때 복제는 아직 설정되기 전**입니다. ProxySQL의 replication hostgroup은 read_only=ON만 보고 이 노드를 즉시 reader 풀(HG20)에 편입시키므로, **복제가 한참 밀린(또는 끊긴) 낡은 데이터를 읽어버리는 stale read**가 발생합니다.
>
> `max_replication_lag`은 "복제가 동작 중인데 지연된" 경우는 막지만, "복제가 아예 설정 전"인 노드는 못 막습니다. 따라서 노드 재투입은 다음 절차로 처리합니다.
> - **격리(`db-isolate.sh`):** mysqld 시작 전 ProxySQL에서 OFFLINE_SOFT + Orchestrator begin-downtime
> - **재투입(`db-rejoin.sh`):** mysqld 시작 후 복제 설정 → catch-up 검증 → 지연이 임계치 이하일 때만 ONLINE 전환
>

<br />

## 5. 동작 흐름 및 트러블슈팅

### 정상 동작 흐름

1. 애플리케이션이 ProxySQL(6033)에 접속합니다.
2. ProxySQL이 쿼리를 분석하여 쓰기·잠금 읽기는 HG10(Primary), 일반 읽기는 HG20(Replica)으로 라우팅합니다.
3. Orchestrator 페일오버로 새 Primary의 read_only가 해제되면, ProxySQL이 이를 감지해 해당 노드를 HG10으로 자동 이동합니다.
4. PostFailover 훅이 신 Primary의 ProxySQL 상태를 ONLINE으로 보장합니다.

### 라우팅 통계 확인

```sql
SELECT hostgroup, digest_text, count_star
FROM stats_mysql_query_digest
ORDER BY count_star DESC LIMIT 10;
```

### 트러블슈팅

| 증상 | 원인 | 조치 |
| --- | --- | --- |
| FOR UPDATE가 reader로 라우팅됨 | 쿼리 룰 순서 오류 | rule 100(FOR UPDATE)을 rule 200보다 먼저 배치 |
| 재기동 노드가 낡은 데이터 반환 | 복제 설정 전 read_only=ON 노드가 reader 편입 | `db-isolate.sh`/`db-rejoin.sh` 재투입 절차 적용 |
| 지연된 Replica가 읽기를 받음 | max_replication_lag 미설정 | HG20에 max_replication_lag 설정 |
| 페일오버 후 트래픽이 신 Primary로 안 감 | 신 Primary 상태가 OFFLINE | PostFailover 훅에서 ONLINE 보장 (Orchestrator 문서 참조) |
