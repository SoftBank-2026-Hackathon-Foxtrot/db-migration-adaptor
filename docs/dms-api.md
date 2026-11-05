# GCP DMS와 공통 API

복사·CDC·승격은 GCP DMS가 수행합니다. DB Operator는 같은 작업 큐에서 DMS 실행 결과를 확인하고 PgBouncer 경로를 바꿉니다. 앱은 등록 때 받은 PgBouncer 주소를 계속 사용합니다. DMS 실패 시 자체 복제로 바꾸지 않습니다.

현재 프로필은 `gcp-dms-postgres-pgbouncer-v1`입니다. RDS PostgreSQL → Cloud SQL PostgreSQL의 연속 복제 작업을 사용합니다. 기존 `postgres-logical-pgbouncer-v1`은 별도로 선택하는 자체 복제 랩 경로입니다.

## 먼저 준비할 것

- 동적 binding 모드, 전용 RDS·Cloud SQL 인스턴스, 업무 DB·writer 역할, Secret과 접근 경로.
- DMS 작업과 원본·대상 연결 프로필. 작업은 `NOT_STARTED`, 원본 writer는 LOGIN, 대상 writer는 NOLOGIN이어야 합니다.
- 원본·대상의 같은 DB 이름과 업무 비밀번호. 대상 writer는 관리자·소유자·상속 역할을 사용하지 않습니다.
- 등록할 전체 table·sequence. DDL·미등록 writer·관리 작업은 이전 중 금지합니다.
- GCP 인증과 DMS 조회·연결 검사·시작·승격·정지·작업 삭제 권한, Cloud SQL 인스턴스 조회 권한(`cloudsql.instances.get`). AWS Task Role만으로 GCP 권한이 생기지는 않습니다.

관리형 DB의 `provider`는 `AWS_RDS` 또는 `GCP_CLOUDSQL`입니다. `provider_instance`에는 RDS ARN 또는 `projects/PROJECT/instances/INSTANCE`를 넣습니다. `identity`는 `PROVIDER:INSTANCE/DATABASE`입니다. 플랫폼이 실제 클라우드 인스턴스 식별자를 확인해서 등록해야 합니다. 관리 DSN에는 `sslmode=verify-full`과 CA를 사용합니다.

DMS 연결 프로필의 원본 host·port·복제 사용자와 대상 Cloud SQL ID·주소를 등록값과 대조합니다. 기존 Cloud SQL의 `postgresql.cloud_sql_id` 형식도 처리합니다. 주소가 프로필에 없으면 [Cloud SQL 인스턴스 GET](https://docs.cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/instances/get)으로 확인하며 OUTGOING 주소를 접속 주소로 사용하지 않습니다. 관리형 DB 등록은 `pg_control_system()` 대신 플랫폼이 게시한 인스턴스 식별자를 사용합니다. 이 검사는 클라우드 메타데이터 조회를 대신하지 않습니다.

DMS 승격은 인스턴스 범위에 영향을 줍니다. 현재 프로필은 전용 인스턴스만 사용합니다. 다른 업무 DB·binding이 있거나 기본 postgres DB에 업무 데이터가 있으면 거절합니다. 여러 DB의 개별 승격은 구현하지 않았습니다.

## 작업 등록

플랫폼 백엔드가 DB와 업무 범위를 게시한 뒤 DMS 작업도 게시합니다. 이 SDK는 클라우드 자원을 생성하지 않습니다.

```python
from db_operator.config import ResourceCatalog
from db_operator.dms import JobBinding

catalog = ResourceCatalog(store)
catalog.dms_job(
    "dms:reservation",
    source_ref="db:reservation-rds",
    target_ref="db:reservation-cloudsql",
    binding=JobBinding(
        job="projects/PROJECT/locations/REGION/migrationJobs/JOB",
        source="projects/PROJECT/locations/REGION/connectionProfiles/SOURCE",
        destination="projects/PROJECT/locations/REGION/connectionProfiles/TARGET",
        owner="foxtrot",
    ),
)
```

작업의 `foxtrot-owner` label은 등록한 owner와 같아야 합니다. `CATALOG_DMS`는 다른 승인 리소스와 같은 `<namespace>:control` 영역에 저장합니다. Operator는 읽기 권한만 받습니다.

앱을 [binding API](bindings.md)로 등록한 뒤 이전 계획을 만듭니다.

```json
{
  "workflowId": "wf-reservation",
  "resourceId": "database",
  "kind": "DATABASE",
  "profile": "gcp-dms-postgres-pgbouncer-v1",
  "replicationRef": "dms:reservation",
  "sourceRef": "db:reservation-rds",
  "targetRef": "db:reservation-cloudsql",
  "bindingRef": "등록된 bindingId",
  "workloadScopeRef": "binding 조회에서 받은 workloadScopeRef",
  "policy": {
    "rollbackMode": "BEFORE_TARGET_WRITE",
    "maxQuiesceSeconds": 300
  }
}
```

## 실행 순서

| 공통 명령 | 실제 처리 |
|---|---|
| PREPARE | DB·프록시·DMS 소유와 접속 정보 검사 → 복제용 barrier 준비 → DMS Verify 완료 확인 |
| START_SYNC | DMS Start → 시작 요청의 실행 결과 확인. 초기 복사 완료는 GET에서 별도 확인 |
| GET 상태 | DMS가 정상 RUNNING·CDC이면 READY. READY는 최종 무손실 전환 보장이 아님 |
| QUIESCE | 업무 정지 근거 확인 → PgBouncer PAUSE → 원본 writer NOLOGIN → barrier 적용·schema·데이터 대조 |
| CUTOVER | 정지·검증 재확인 → DMS Promote 완료 확인 → sequence·대상 writer 권한 준비 → 최종 대조 → PgBouncer 대상 경로 설정 |
| RESUME TARGET | 대상 writer LOGIN → PgBouncer RESUME → 앱 계정의 실제 DB 접속 검사 |
| FINALIZE | 업무·안정 구간·보존 기간 확인 → 계획 종료. 앱 접속점·DB·데이터 유지 |

앱·Worker·예약 작업 정지와 재개는 오케스트레이터가 처리합니다. 정지 근거는 10초 이하 간격으로 갱신합니다. Operator가 접속을 열어도 앱 업무 재개와 실제 업무 검사는 별도로 수행해야 합니다.

최종 검증은 등록한 table의 전수 digest를 대조합니다. 큰 DB에서는 이 검사가 쓰기 정지 시간을 늘릴 수 있습니다. 기존 자체 복제 성능 수치를 DMS 전환 시간으로 사용하지 않습니다.

DMS Promote는 대상 DB를 원본과 분리합니다. 원본 쓰기를 멈추고 변경 적용을 기다린 뒤 승격합니다. PgBouncer가 있어도 쓰기 정지 구간이 없어지는 것은 아닙니다. [GCP 승격 절차](https://docs.cloud.google.com/database-migration/docs/postgres/promote-migration)

## 실패와 롤백

- 대상 쓰기 허용 전: 업무 정지를 유지하고 ROLLBACK → RESUME SOURCE. DMS가 실행 중이면 먼저 Stop 결과를 확인합니다.
- 대상 쓰기 허용 후: 역복제가 없으므로 원본 복귀를 거절합니다. 이 프로필은 `REVERSE_REQUIRED` 요청도 거절합니다. GCP DMS의 Restart를 롤백으로 사용하지 않습니다.
- 완료 이력 조회·동일 요청 재전송: 저장된 결과를 반환하며 GCP 인증을 새로 읽지 않습니다. 해제한 접속점의 완료 기록도 조회할 수 있습니다.
- DMS 응답 유실·시간 초과: UNKNOWN에서 멈춥니다. RECONCILE은 기존 실행 결과를 읽으며 변경 요청을 다시 보내지 않습니다.
- 클라우드 실행 ID까지 유실: 감사 기록에서 확인한 `providerOperationRef`를 RECONCILE에 전달합니다. 프로젝트·리전·작업·명령·접수 시각이 맞아야 연결합니다.
- 승격 RPC 성공, 작업은 아직 미완료: 대상 쓰기를 열지 않습니다. 기존 승격이 끝나는지 확인합니다.
- 취소·원본 복귀 후 FINALIZE: 정지된 DMS 작업을 강제 옵션 없이 삭제하여 남은 복제를 정리합니다. 대상 인스턴스 삭제 요청은 보내지 않습니다. GCP는 작업 삭제 시 대상 연결 프로필도 삭제합니다. [삭제 API](https://docs.cloud.google.com/database-migration/docs/reference/rest/v1/projects.locations.migrationJobs/delete)

같은 DMS 작업은 같은 DynamoDB namespace를 사용해야 합니다. API 계획이 점유한 작업을 별도 CLI로 변경하면 거절합니다. 클라우드 콘솔의 외부 변경도 이전 중 금지합니다.

## ECS 인증

로컬에서는 `GOOGLE_APPLICATION_CREDENTIALS` 또는 ADC를 사용합니다. ECS 생성 설정의 선택 항목 `gcp_credentials_secret_arn`은 GCP 인증 JSON을 보관한 Secrets Manager ARN입니다. backend에만 주입합니다. 실행 역할에 해당 Secret 읽기 권한을 줍니다.

실행기는 JSON을 권한 0600의 `/run/foxtrot/google-credentials.json`으로 기록하고 Google SDK에 파일 경로를 전달합니다. 인증 JSON은 DynamoDB·API 응답·로그에 저장하지 않습니다. 외부 계정 인증 설정을 쓰면 그 설정이 요구하는 토큰·인증 경로도 배포 환경에서 제공해야 합니다.

## 검증 범위

API·Worker·SQL·PgBouncer 연결은 로컬 DB·DynamoDB Local·모의 GCP SDK로 검사합니다. 실제 DMS 초기 복사·CDC를 측정했던 이전 결과와 이번 공통 API 전환 검사는 별개입니다. 실제 RDS·Cloud SQL의 권한, DMS 승격, ECS 인증과 공인망 연결은 추가 검증이 필요합니다.
