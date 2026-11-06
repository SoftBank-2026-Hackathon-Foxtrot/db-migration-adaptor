# RDS → Cloud SQL 직접 논리 복제

현재 기본 연결은 [Redis와 명령 의미를 맞춘 v2](atomic-cutover.md)입니다. 아래 구현 설명과 클라우드 실측은 v1의 기록입니다. 기존 v1 계획은 동작을 유지합니다.

## 구현 범위

`postgres-managed-logical-pgbouncer-v1` 프로필을 구현했습니다. RDS PostgreSQL 17 → 독립 Cloud SQL PostgreSQL 17을 대상으로 합니다. DMS 작업과 Promote는 사용하지 않습니다. 기존 DMS와 PostgreSQL 18 랩 프로필은 별도로 유지합니다.

공통 API의 PREPARE → START_SYNC → QUIESCE → CUTOVER → RESUME 순서는 같습니다. 대상 schema와 역할은 준비한 뒤 등록합니다. PostgreSQL이 초기 복사와 CDC를 수행하고, Operator가 publication·subscription과 PgBouncer를 제어합니다.

## 전환 조건

원본 쓰기 차단 → 최종 barrier의 내구성 있는 적용 확인 → subscription 비활성화·작업 종료 확인 → sequence 맞춤 → 원본 복제 slot 정리 → PgBouncer 목적지 변경 → 대상 쓰기 허용 순서입니다.

정지 중에는 table 전수 비교를 하지 않습니다. 등록한 전체 table의 초기 동기화 완료, 단일 subscription, 최종 barrier, 적용 LSN, writer 차단과 경로를 전환 근거로 사용합니다. DDL과 미등록 writer는 이전 중 허용하지 않습니다. 원본·대상 데이터 전체의 동일성을 검사한 결과로 표현하지 않습니다.

승격 API를 호출하지 않아도 원본 차단과 복제 완료 확인은 필요합니다. 이번 실측은 아래 조건의 한 번의 결과이며 전환 시간의 상한을 보장하지 않습니다.

## 등록과 접속

앱 binding과 DB·table·sequence를 등록한 뒤 다음 계획을 만듭니다. `replicationRef`는 넣지 않습니다.

```json
{
  "workflowId": "wf-reservation",
  "resourceId": "database",
  "kind": "DATABASE",
  "profile": "postgres-managed-logical-pgbouncer-v1",
  "sourceRef": "db:source",
  "targetRef": "db:target",
  "bindingRef": "등록된 bindingId",
  "workloadScopeRef": "binding에 등록된 업무 범위",
  "policy": {"rollbackMode": "BEFORE_TARGET_WRITE", "maxQuiesceSeconds": 60}
}
```

대상 Cloud SQL의 실제 인스턴스·주소·독립 DB 상태를 SQL Admin API로 확인합니다. DMS 복제 대상이나 읽기 복제본은 거절합니다. 원본에는 `rds_superuser`·복제 역할과 logical WAL 설정이 필요하고, 대상의 subscription 관리에는 `cloudsqlsuperuser` 역할이 필요합니다. 앱 writer는 관리 권한이나 table 소유권을 갖지 않습니다.

앱·Operator의 관리형 DB 접속은 `verify-full`을 사용합니다. 실제 시험에서 Cloud SQL 서버가 RDS로 연결하는 subscription은 `sslmode=require`로 암호화했습니다. 이 설정은 CA와 호스트 이름을 검증하지 않습니다. 로컬 CA 파일 경로는 Cloud SQL 서버에서 사용할 수 없으므로 그대로 전달하지 않습니다. 운영에서 복제 연결의 서버 인증이 필요하면 별도 신뢰 경로를 준비해야 합니다. 기존 앱·관리 접속의 인증서 검증은 낮추지 않았습니다.

Cloud SQL은 `pg_subscription.subconninfo` 조회를 허용하지 않습니다. 실행 전에 복제 연결의 해시를 저장하고, subscription 소유자·publication·slot과 대조합니다. 이 검사는 외부 관리자가 접속 문자열을 바꾼 사실까지 확인하지 못합니다. 이전 중 외부 관리 변경은 금지합니다.

한 action 안에서 DB 연결을 재사용하되 autocommit으로 실제 상태를 다시 읽습니다. action이 끝나면 연결을 닫습니다. 연결 실패를 캐시된 성공으로 처리하지 않습니다.

## 실패와 복귀

외부 변경 전 실행 의도를 기록합니다. 응답 유실은 UNKNOWN에서 멈추고 RECONCILE로 실제 상태를 대조합니다. 대상 쓰기 허용 전에는 원본으로 복귀할 수 있습니다. 이 프로필의 첫 범위는 `BEFORE_TARGET_WRITE`이며, 대상 쓰기 후 원본 복귀와 역방향 CDC는 지원하지 않습니다.

## 완료 조건

- PostgreSQL 17에서 초기 복사와 동시 INSERT·UPDATE·DELETE·ROLLBACK을 검증합니다.
- 최종 barrier 미적용, 복제 작업 잔존, 정지 근거 만료 시 대상 쓰기를 열지 않습니다.
- 전환 후 대상의 새 쓰기, sequence 증가와 원본 writer 차단을 확인합니다.
- DMS 호출 없이 동작해야 합니다.
- 별도 테스트 DB에서 쓰기 정지부터 대상 첫 쓰기까지 실측합니다. 기존 시연과 업무 데이터를 보존합니다.

## 2026-11-06 검증 결과

실제 RDS와 기존 소형 Cloud SQL에 별도 업무 DB를 만들었습니다. 초기 주문 50,000건·상품 128개로 시작했고, 원본 주문 생성을 계속한 상태에서 복사와 CDC를 확인했습니다. 전환 시 원본 주문은 50,849건이었습니다.

| 구간 | 실측 |
|---|---:|
| 쓰기 정지 → QUIESCE 완료 | 3.03초 |
| CUTOVER 완료까지 추가 시간 | 2.02초 |
| RESUME 완료·앱 쓰기 재개까지 추가 시간 | 2.46초 |
| 쓰기 정지 → 앱 쓰기 재개 | **7.50초** |
| 쓰기 정지 → 대상 첫 쓰기 확인 | **7.91초** |

DMS와 Promote는 호출하지 않았습니다. 전환 후 ACTIVE / OK, 원본 writer NOLOGIN, 대상 writer LOGIN, 원본 slot·publication 정리, 대상 subscription 비활성화와 작업 종료를 확인했습니다. 복사 중 추가한 INSERT·UPDATE·DELETE·ROLLBACK 결과와 재고+주문 수 불변식도 대조했습니다. 대상에서 새 주문이 계속 성공하며 쓰기 오류는 0건입니다.

기존 DMS의 67.18초는 주문 549,370건·원본 추가 쓰기 0건 조건입니다. 이번 결과와 데이터·부하 조건이 다르므로 단축 비율이나 일반적인 전환 성능으로 표현하지 않습니다. 새 인스턴스 생성·schema 준비·초기 복사 시간도 7.50초에 포함되지 않습니다.

관련 검사 **120개**가 통과했습니다. 단위 69개, PostgreSQL 17 관리형 프로필 통합 5개, 기존 자체 복제·앱 binding·DMS 통합 46개입니다. 이 중 로컬 관리형 통합 검사의 Cloud SQL 메타데이터 응답은 모의이며, 실제 클라우드 확인은 위 별도 실행 결과입니다. 전체 테스트 모음을 모두 실행한 결과는 아닙니다.

실측 근거는 [성능 JSON](performance/2026-11-06-native.json)에 있습니다. 로컬 시연은 `http://127.0.0.1:5193/`입니다. 기존 시연과 원래 RDS 업무 데이터는 보존했습니다. 기존에 중지한 벤치마크 Cloud SQL을 재사용해 실행 중이며, 추가 인스턴스와 DMS 작업은 만들지 않았습니다.

### 미등록 앱부터 전체 실행 재측정

별도 테스트 DB에서 원본 주문 50,000건·상품 128개, 대상 주문 0건으로 시작했습니다. 앱 binding과 migration이 없는 상태에서 `MIGRATE_ALL` 한 번으로 등록 → 사전 검사 → 초기 복사·CDC → 쓰기 정지 → 경로 변경 → 앱 재개를 실행했습니다. 복사 중 원본 주문 생성은 계속했고 전환 시 원본 주문은 50,022건이었습니다.

| 구간 | 실측 |
|---|---:|
| 앱 등록 시작 → 앱 쓰기 재개 | **8.58초** |
| 앱 등록 시작 → 대상 첫 주문 커밋 | **8.78초** |
| 쓰기 정지 → 앱 쓰기 재개 | **2.82초** |

DB·schema 준비와 컨테이너 시작은 이 시간에 포함하지 않습니다. 기존 7.50초 기록은 READY 상태에서 시작한 별도 쓰기 정지 측정이며 전체 실행 시간이 아닙니다. 이번 결과도 초기 5만 건 조건의 1회 실측입니다.

대상 새 주문과 오류 0건, 원본 writer 차단, subscription 비활성화·작업 종료, 원본 slot·publication 정리, 재고+주문 불변식을 확인했습니다. 원래 RDS 업무 데이터와 두 기존 시연을 보존했습니다. 측정용 컨테이너와 접속 허용 규칙만 정리했습니다. [전체 실행 성능 JSON](performance/2026-11-06-native-e2e.json)에 시각과 범위를 기록했습니다.

## 근거

- [RDS 기본 논리 복제](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html)
- [Cloud SQL 기본 논리 복제](https://docs.cloud.google.com/sql/docs/postgres/replication/configure-logical-replication)
- [PostgreSQL schema·sequence 제한](https://www.postgresql.org/docs/17/logical-replication-restrictions.html)
- [복제 적용 위치 추적](https://www.postgresql.org/docs/17/replication-origins.html)
