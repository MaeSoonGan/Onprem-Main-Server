# ⚙️ 계정계 애플리케이션 배포 (체결 엔진 · 원장시스템)

> 체결 엔진과 원장시스템의 애플리케이션 로직 및 API 명세는 별도 레포(Backend On-Prem)에서 관리합니다. 본 문서는 주센터 인프라 위에서 두 애플리케이션을 **어떻게 배치하고 서비스로 운영하는가**를 다룹니다.

## 1. 개요 및 프로젝트 내 역할

체결 엔진과 원장시스템은 주센터 Core Cluster에서 동작하는 계정계의 핵심 애플리케이션입니다.

- **체결 엔진:** 주문을 받아 체결 여부를 판단하고, KIS Developers API 시세를 연동하며, 체결 결과를 Kafka 주문 이벤트로 발행합니다.
- **원장시스템:** 주문/체결 결과를 원장 DB에 기록하는 단일 진실 소스(Single Source of Truth)입니다.

두 애플리케이션 모두 Java 기반으로 빌드된 jar 산출물을 Core Cluster VM에 배치하고, **systemd 서비스로 등록하여 운영**합니다. 인프라 관점에서 배포·기동·상태 관리 방식이 동일하므로 본 문서에서 함께 다룹니다.

<br />

## 2. 구성 환경 및 배치

| 구분 | 내용 |
| --- | --- |
| 배치 위치 | 주센터 Core Cluster VM |
| 실행 산출물 | Java 애플리케이션 jar |
| 실행 방식 | systemd 서비스 (`systemctl`로 기동/중지/상태 관리) |
| 체결 엔진 외부 연동 | KIS Developers API (REST + WebSocket) |
| 체결 엔진 → 원장시스템 | Kafka 주문 이벤트 |
| 원장시스템 → DB | MySQL (ProxySQL 경유) |

<br />

## 3. 배포 방식 — systemd 서비스 등록

빌드된 jar를 대상 VM에 전달한 뒤, systemd 유닛 파일로 등록하여 서비스로 운영합니다. 이렇게 하면 부팅 시 자동 기동, 비정상 종료 시 재시작, 표준 로그(journald) 수집이 일관되게 적용됩니다.

### 3-1. systemd 유닛 파일 예시

```ini
# /etc/systemd/system/matching-engine.service
[Unit]
Description=MaeSoonGan Matching Engine
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=appuser
WorkingDirectory=/opt/matching-engine
ExecStart=/usr/bin/java -jar /opt/matching-engine/matching-engine.jar
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

> 원장시스템도 동일한 구조의 유닛 파일(`ledger-system.service`)로 등록합니다. 경로·jar 이름·서비스명만 다릅니다.

### 3-2. 배포 및 운영 명령

```bash
# 유닛 파일 반영
sudo systemctl daemon-reload

# 부팅 시 자동 기동 등록
sudo systemctl enable matching-engine

# 기동 / 중지 / 재시작
sudo systemctl start matching-engine
sudo systemctl stop matching-engine
sudo systemctl restart matching-engine

# 상태 및 로그 확인
sudo systemctl status matching-engine
sudo journalctl -u matching-engine -f
```

### 3-3. 신규 버전 배포 흐름

1. 새 jar를 대상 VM의 배포 경로(`/opt/<app>/`)로 전달합니다.
2. `systemctl stop <service>`로 기존 서비스를 정지합니다.
3. jar를 교체합니다.
4. `systemctl start <service>`로 재기동하고 `status`/`journalctl`로 정상 기동을 확인합니다.

<br />

## 4. 동작 흐름 및 트러블슈팅

### 정상 동작 흐름

1. Core Cluster VM에서 체결 엔진·원장시스템 jar가 각각 systemd 서비스로 상시 기동되어 있습니다.
2. 체결 엔진은 KIS API 시세를 받아 주문을 처리하고 Kafka로 이벤트를 발행합니다.
3. 원장시스템은 Kafka 이벤트를 소비하여 ProxySQL 경유로 MySQL 원장 DB에 기록합니다.
4. 두 서비스 모두 비정상 종료 시 systemd가 자동 재시작합니다.

### 트러블슈팅

| 증상 | 원인 | 조치 |
| --- | --- | --- |
| 서비스가 기동 직후 종료됨 | jar 경로 오류, 포트 충돌, 의존 서비스(DB·Kafka) 미기동 | `journalctl -u <service>`로 스택트레이스 확인 후 원인 해소 |
| 부팅 후 서비스가 자동 기동되지 않음 | `systemctl enable` 누락 | `systemctl enable <service>` 적용 |
| 배포 후 구버전 동작 | jar 교체 후 미재시작 | `systemctl restart <service>` 적용 확인 |
| 체결 엔진이 DB·Kafka에 붙지 못함 | 의존 서비스 기동 순서 문제 | 유닛의 `After=`/`Wants=` 의존성 점검, 의존 서비스 기동 확인 |
