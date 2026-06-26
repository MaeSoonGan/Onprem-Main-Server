# 🔧 CI/CD 구성

## 1. 개요 및 프로젝트 내 역할

CI/CD Cluster는 소스 관리부터 정적 분석, 아티팩트·이미지 저장, 시크릿 관리, 배포 자동화까지 이어지는 파이프라인 도구들을 운영하는 영역입니다. 금융권에서 표준적으로 갖추는 빌드·배포 체인을 온프레미스에 구성하여, 코드가 외부로 나가지 않는 폐쇄망 환경에서도 빌드 결과물과 시크릿을 자체적으로 관리할 수 있도록 했습니다.

CI/CD Cluster는 물리 호스트 위의 **중첩 ESXi 호스트**에 별도 VM들로 구성되며, OS는 일관성을 위해 모두 Rocky Linux 9로 통일했습니다.

| 항목 | 내용 |
| --- | --- |
| 배치 | 중첩 ESXi (CI/CD 전용 호스트 VM) |
| OS | Rocky Linux 9 minimal |
| VLAN | CI/CD 전용 VLAN |
| 게이트웨이/라우팅 | pfSense (NAT, inter-VLAN 라우팅) |

<br />

## 2. 구성 요소 및 역할

| 도구 | 역할 |
| --- | --- |
| GitLab CE | 소스 저장소 및 CI 파이프라인 중심 (`.gitlab-ci.yml`로 빌드→테스트→배포 정의) |
| GitLab Runner (Onprem-runner) | 파이프라인 잡을 실제 실행하는 워커. 내부망의 DB·Core VM·EKS에 접근 가능 |
| SonarQube Community | 정적 코드 분석 — 버그·코드 스멜·취약점 스캔, 품질 게이트 |
| Nexus 3 | Maven/Gradle JAR 등 빌드 아티팩트 저장소 및 외부 라이브러리 프록시 캐시 |
| Harbor | 컨테이너 이미지 프라이빗 레지스트리 (Trivy 취약점 스캔 내장) |
| HashiCorp Vault | DB 비밀번호·API 키 등 시크릿 중앙 관리 (코드 하드코딩 방지) |
| Ansible AWX | Ansible 플레이북의 웹 UI/API — VM 프로비저닝·배포 자동화 |

> 초기에는 Jenkins + Ansible 조합을 검토했으나, GitLab CI + AWX 조합으로 전환하여 소스 관리와 파이프라인을 GitLab으로 일원화했습니다.

<br />

## 3. 설치 방식 — 하이브리드 접근

도구 특성에 맞춰 설치 방식을 나눴습니다.

| 도구 | 설치 방식 | 비고 |
| --- | --- | --- |
| GitLab CE | Omnibus RPM 패키지 | 단일 패키지로 의존성 일괄 설치 |
| SonarQube / Nexus | Docker Compose | 한 VM(devtools)에서 함께 운영, PostgreSQL 연동 |
| Harbor | 오프라인 인스톨러 스크립트 | 내부적으로 자체 docker-compose.yml 생성 (SonarQube/Nexus와 별도 관리) |
| Vault | Docker Compose | UID 100 컨테이너로 동작 |

> 버전은 설치 시점에 웹 검색으로 최신 안정 버전을 확인하여 적용했습니다. (예: Harbor는 당시 최신인 v2.15.0 적용)

<br />

## 4. 파이프라인 흐름 (설계)

표준적인 빌드·배포 체인은 다음 순서로 설계했습니다.

1. 개발자가 소스를 GitLab에 push합니다.
2. GitLab이 파이프라인을 트리거하고, Onprem-runner가 빌드를 실행합니다.
3. 빌드 단계에서 Vault로부터 시크릿(DB 비밀번호·API 키)을 주입받습니다.
4. SonarQube가 정적 분석을 수행하고 품질 게이트를 평가합니다.
5. 빌드 산출물은 Nexus(JAR), 컨테이너 이미지는 Harbor에 저장합니다.
6. Ansible AWX가 Core VM 또는 AWS EKS에 배포를 수행합니다.

<br />

## 5. 구축 범위에 대한 메모

CI/CD Cluster의 각 도구는 위와 같이 설치·구동까지 완료하여 표준 파이프라인 체인을 구성했습니다. 다만 이번 프로젝트의 핵심 검증 목표는 **주센터 전원 차단 시 DR 자동 페일오버**였으므로, 발표·시연에서는 DB HA와 DR 시나리오에 집중했습니다.

<br />

## 6. 트러블슈팅

| 도구 | 증상 | 원인 | 조치 |
| --- | --- | --- | --- |
| Vault | 커스텀 listener 설정이 적용되지 않음 | 기본 entrypoint(`docker-entrypoint.sh`)가 8200 포트를 선점 | `entrypoint: ["vault"]`로 오버라이드, `/etc/vault.d`를 `/dev/null`로 마스킹 |
| Vault | data 디렉터리 권한 오류 | 컨테이너가 UID 100으로 동작 | `chown -R 100:100 data/` 적용 |
| Vault | 재시작 시 시크릿 소실 / 토큰 변경 | dev 모드는 인메모리 + 자동 unseal, 재시작 시 초기화 | 운영 목적 시 file-storage 운영 모드로 전환(재시작 후 수동 unseal 필요) |
| 중첩 ESXi VM | VLAN 트래픽 통신 불가 | 상위 ESXi 포트그룹 보안 정책 미개방 | Promiscuous / MAC Address Changes / Forged Transmits를 Accept로 설정 |
