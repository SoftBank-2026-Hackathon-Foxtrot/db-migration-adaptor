# 양방향 이전과 정지 구간 단축 (2026-11-08)

`postgres-managed-logical-pgbouncer-v2`를 RDS ↔ Cloud SQL 어느 방향으로도 반복 실행할 수 있게 하고,
CUTOVER 한 명령 안의 쓰기 정지 구간을 줄입니다. 기록은 작업 순서대로 갱신합니다.

## 출발점

- 2026-11-08 02:46 KST 클라우드 E2E(wf-55a54889)에서 RDS → Cloud SQL 전환의 앱 정지 구간은 약 12초였습니다. 단계별 시간은 기록되지 않았습니다.
- 코드는 원본 `AWS_RDS`, 대상 `GCP_CLOUDSQL`만 받습니다(`service.create`, `ManagedLogicalPostgres.preflight`). 대상 메타데이터 검사는 Cloud SQL Admin API 전용입니다.
- 되돌아가는 이전은 대상(이전 원본)의 업무 table이 비어 있지 않아 `TARGET_NOT_EMPTY`로 막힙니다. 이전 이전의 복제 객체가 남아 있으면 `_check_isolation`도 막습니다.
- 정지 구간 안에서 실행되는 것: 관측(`observe`) 5회, `validate` 3회, 양쪽 schema 전체 서명 2회(table마다 6개 질의), 실행 의도 기록마다 DynamoDB 트랜잭션 2회, 50ms 폴링.

## 단계

| # | 내용 | 상태 |
|---|---|---|
| 1 | 방향 제한 제거: 원본·대상 모두 관리형(RDS, Cloud SQL)이면 허용. 인스턴스 메타데이터는 제공자별 검사(Cloud SQL Admin API, RDS DescribeDBInstances). 원본 logical WAL 필수 | 완료 |
| 2 | 되돌아가는 이전: 완료(COMPLETED)된 이전 이전의 원본이던 대상을 PREPARE에서 초기화. 대상 writer NOLOGIN·세션 0·경로 SOURCE를 확인한 뒤 등록 table TRUNCATE와 barrier 삭제. 완료된 이전의 복제 객체 잔여물도 정리 | 완료 |
| 3 | 정지 구간 단계별 시간 기록(`internal.cutoverTimeline`, `writePauseMs`)과 관측 API 노출 | 완료 |
| 4 | 정지 구간 단축: 실행 의도 기록과 정지 근거 검사를 한 트랜잭션으로, 정지 중 schema 검사를 한 질의 digest로, 같은 명령 안의 관측·validate 재사용, 폴링 10ms | 완료 |
| 5 | 로컬 랩(PostgreSQL 17) 양방향 통합 검사와 전체 검사 통과 | 완료 |
| 6 | IaC: RDS가 Cloud SQL 인증서 이름을 풀 수 있도록 Route53 private zone을 앱 VPC에도 연결, db-proxy에 `rds:DescribeDBInstances` | 커밋, apply 대기 |

## 결과

### 양방향

- `service.create`와 `ManagedLogicalPostgres.preflight`는 원본·대상이 모두 `AWS_RDS` 또는 `GCP_CLOUDSQL`이면 받습니다. 두 인스턴스 모두 제공자 API로 실행 중인 독립 인스턴스인지 확인합니다(RDS는 `DescribeDBInstances`, 읽기 복제본 거절). db-proxy task role에 `rds:DescribeDBInstances`가 필요합니다(IaC `youngjin/11_08_03_40`).
- 양쪽 모두 `wal_level=logical`이 필요합니다. IaC의 RDS parameter group과 Cloud SQL flag가 이미 그렇게 둡니다.
- 되돌아가는 계획(최근 COMPLETED 이전의 target → source)은 PREPARE에서 `pg.reset_retired_target` 의도를 기록한 뒤 대상의 등록 table을 TRUNCATE하고 barrier를 지웁니다. 조건: 앱 경로가 원본, 대상 writer NOLOGIN, 대상 세션·prepared 0. 이 조건 밖의 비어 있지 않은 대상은 그대로 `TARGET_NOT_EMPTY`입니다. 완료된 이전이 남긴 `fxt_*` subscription·slot·publication도 같은 때 정리합니다.
- Cloud SQL이 원본이면 RDS가 subscriber가 되어 `replicationDsn`의 `*.sql.goog` 이름을 풀어야 합니다. 플랫폼 Route53 private zone을 앱 VPC에도 연결하는 IaC 변경을 커밋했습니다(`youngjin/11_08_03_40`, 아직 apply 전). apply 때 live Tailscale ACL의 on-prem 규칙이 Terraform에 없어 전체 apply는 그 규칙을 지우므로, 두 리소스만 `-target`으로 적용해야 합니다. 경로는 기존 Tailscale 라우터의 VPC 경로(10.21.0.0/16)와 GCP 쪽 10.10.0.0/16 export를 그대로 씁니다.

### 정지 구간

PostgreSQL 17 로컬 랩(왕복 지연 없음, DynamoDB Local)에서 `writePauseMs`(PAUSE 의도 → RESUME 의도):

| | 1회 | 2회 | 3회 |
|---|---:|---:|---:|
| 변경 전 | 324 | 335 | 337 |
| 변경 후 | 307 | 293 | 264 |

로컬은 DynamoDB 호출이 대부분이라 차이가 작습니다. 클라우드(ap-northeast-2 ↔ asia-northeast3, 왕복 30~40ms)에서 줄어드는 원격 질의는 다음과 같습니다.

| 항목 | 변경 전 | 변경 후 |
|---|---|---|
| 정지 중 schema 검사 | table마다 6개 질의 × 양쪽(등록 table 6개 기준 약 70회) | DB당 1회 digest |
| 관측(`observe`, 질의 4~5회 + 프록시 1회) | quiesce 2회, cutover 2회, resume 1회 | quiesce 2회, cutover 1회(경로 변경 뒤), resume 0회 |
| `validate_record`(질의 3~4회) | 3회 | 1회 |
| 실행 의도 기록의 DynamoDB 트랜잭션 | 의도마다 2회(정지 근거 검사 + 기록) | 1회(같은 트랜잭션) |
| 폴링 간격 | 50ms | 10ms |

같은 명령 안에서 재사용하는 관측은 마지막 외부 변경(writer 차단, 경로 변경) 직후의 것이고, 재사용하는 검증은 subscription 정지 뒤의 것이라 그 사이에 바뀔 수 없습니다. 별도 명령으로 나뉜 v1 흐름은 매번 새로 관측·검증합니다. 클라우드 재측정은 다음 E2E에서 `lastCutover`로 읽습니다.

### 검증

PostgreSQL 17 관리형 랩에서 `tests/test_managed_logical.py`·`tests/test_atomic.py` 31개 통과: Cloud SQL → RDS 방향 전환, RDS → Cloud SQL 전환 후 FINALIZE → 되돌아가는 계획의 PREPARE 초기화 → 재전환까지 한 번에, 그리고 기존 중단·복구 검사 전부. 전체 검사 모음(두 랩, DMS 벤치마크 제외)은 237개 통과, 1개 건너뜀(7분 35초)입니다.
