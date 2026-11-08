# db-migration-adaptor

PostgreSQL 복제와 PgBouncer를 제어하는 Foxtrot DB Operator입니다. FastAPI는 명령을 DynamoDB에 저장하고, Worker가 복제 완료와 쓰기 차단을 확인한 뒤 프록시의 DB 접속 경로를 바꿉니다.

[Redis와 같은 DB 명령 동작](docs/atomic-cutover.md): 현재 기본 연결은 `postgres-managed-logical-pgbouncer-v2`입니다. QUIESCE는 준비 확인, CUTOVER는 DB 정지·전환·재개, RESUME은 결과 확인입니다. DMS Promote를 사용하지 않습니다.

[RDS → Cloud SQL 직접 논리 복제 v1](docs/managed-logical.md): 기존 프로필과 클라우드 실측 기록입니다. v1의 측정값을 v2 성능으로 표현하지 않습니다.

[GCP DMS API 연결](docs/dms-api.md): 등록할 작업, 실행 순서, 응답 유실 복구와 롤백 범위입니다.

[AWS RDS → 온프렘 PostgreSQL 17](docs/onprem-pg17.md): 비어 있는 자체 운영 DB로 복제하는 전용 프로필, TLS·writer 준비와 요청 예시입니다.

[구현 설명](docs/implementation.md) · [기존 자체 복제 도식](docs/architecture/index.html)

[장애 경로 검토와 수정](docs/review-fixes.md): 설정 보존, 조회 장애, 직접 복귀, LSN 검증, 상태 경합을 보강했습니다.

[ECS 배포 구성](deploy/ecs/README.md): 단일 Task 안의 backend(API·Worker)와 PgBouncer, 경로·정지 상태 복원과 배포 JSON 생성 방법입니다.

[동작·성능 검증 리포트](docs/performance-report.md): 로컬 처리량, 전환·롤백 시간, 발견한 문제와 적용 범위입니다. 실제 RDS ↔ Cloud SQL 양방향 전환의 정지 구간 실측(1.4초)은 [검증 기록](docs/verification.md)에 있습니다.

[여러 앱의 등록·해제](docs/bindings.md): 단일 PgBouncer의 DB별 alias, 승인된 리소스 등록, 연결 종료 확인과 실패 복구 방법입니다.

```text
오케스트레이터 → API → DynamoDB → DB Worker
                                      ├ PostgreSQL: 초기 복사·CDC
                                      ├ 원본·대상 PostgreSQL
앱·업무 Worker → 고정 PgBouncer 주소     └ PgBouncer 관리·설정
```

## 구현 범위

- 현재 선택: `postgres-managed-logical-pgbouncer-v2`. 관리형 PostgreSQL 17(RDS, Cloud SQL) 사이를 어느 방향으로든 이전합니다. `replicationRef` 없이 기본 논리 복제를 사용합니다. 기존 v1도 유지합니다. 같은 binding의 완료된 이전을 되돌리는 계획은 PREPARE에서 이전 원본을 초기화합니다([양방향 이전과 정지 구간](docs/bidirectional-cutover.md)).
- 전환: 업무 정지 근거 → PAUSE → writer 차단 → 최종 barrier·적용 LSN 확인 → subscription 정지 → sequence 맞춤 → 원본 slot 정리 → 경로 변경. 정지 중 table 전수 비교는 하지 않습니다.
- 관리형 롤백: 대상 쓰기 허용 전 원본 복귀. 역복제는 구성하지 않았으며 `REVERSE_REQUIRED`는 거절합니다.
- 선택 가능한 기존 방식: `gcp-dms-postgres-pgbouncer-v1`. 승인한 DMS 작업을 `replicationRef`로 지정합니다. 프로필을 명시적으로 선택하며 실패 시 다른 복제로 자동 변경하지 않습니다.
- 복구: 실행 의도 선기록, 멱등 요청, revision 검사, UNKNOWN 차단과 RECONCILE. 응답 유실 시 기존 클라우드 실행 ID를 대조합니다.
- 진행 판정: 앱 재개·실제 업무 검사·후속 작업과 30초 안정 구간 확인.

기존 `postgres-logical-pgbouncer-v1`은 별도로 선택하는 자체 복제 랩 프로필입니다. 아래 로컬 랩 설명과 이전 역복제·성능 기록은 이 프로필을 기준으로 합니다. 실제 클라우드 DMS와 이번 공통 API 전환의 검증 범위는 [검증 기록](docs/verification.md)에서 구분합니다. 전환 중 쓰기 정지 구간이 있습니다.

## 실행

Python 3.13과 uv를 사용합니다.

```sh
uv sync --locked --dev
```

DMS 배포는 [DMS API 가이드](docs/dms-api.md)의 동적 등록 절차를 따릅니다. 아래는 기존 정적 Registry 실행 예시입니다. registry.example.json과 .env.example을 기준으로 등록합니다. 대상 schema·앱 역할·복제 권한은 미리 준비해야 합니다. 대상 업무 table은 비어 있어야 하고(완료된 이전의 원본으로 되돌아가는 계획은 예외) 대상 업무 계정은 NOLOGIN이어야 합니다. 환경 Secret을 주입한 뒤 다음 명령을 각각 실행합니다.

```sh
uv run --locked foxtrot-db init
uv run --locked foxtrot-db api --host 127.0.0.1 --port 8080
uv run --locked foxtrot-db worker
```

API 문서는 /docs, /redoc, /openapi.json입니다. 서비스 API에는 Authorization: Bearer 토큰이 필요합니다.

## 상태 저장

관리용 PostgreSQL은 사용하지 않습니다. `DB_OPERATOR_STATE_TABLE`, `DB_OPERATOR_STATE_NAMESPACE`, AWS 리전을 설정합니다. AWS에서는 ECS Task Role을 사용합니다.

- API·Worker·PgBouncer는 같은 DynamoDB 테이블과 namespace를 사용합니다.
- 명령·멱등 키·실행 의도·경로 변경을 `TransactWriteItems`로 저장합니다.
- 앱의 업무 SQL은 DynamoDB를 거치지 않습니다.
- Worker 잠금은 자동 만료하지 않습니다. 강제 종료 뒤에는 이전 실행기의 종료를 확인하고 잠금을 복구해야 합니다.
- 기존 PostgreSQL 상태의 자동 이관은 제공하지 않습니다. 진행 중 작업이 있으면 기존 저장소를 보존하고 먼저 종료·대조해야 합니다. 빈 DynamoDB를 기존 작업 상태로 간주하면 안 됩니다.

```sh
uv run --locked foxtrot-db locks
# 이전 Worker의 실제 종료를 확인한 뒤 실행합니다.
uv run --locked foxtrot-db recover-lock \
  --lock-owner PREVIOUS_OWNER --stopped-evidence 'ecs:OLD_TASK_ARN:STOPPED'
```

상태 키·권한·경합 처리는 [구현 설명](docs/implementation.md#dynamodb-저장-계약)에 있습니다.

## 로컬 시험

### Docker 대시보드

새 명령 동작으로 시연하려면 새 구성의 `config.json`에 `profile: postgres-managed-logical-pgbouncer-v2`를 지정하고 이미지를 다시 빌드합니다. DMS 작업은 등록하지 않습니다. 기존 v1 시연의 구성·계획은 유지합니다. 접속 조건과 이전 실측은 [관리형 논리 복제 문서](docs/managed-logical.md)에 있습니다. 아래 DMS 구성과 모의 시연은 기존 방식을 시험할 때 사용합니다.

실제 RDS·Cloud SQL·GCP DMS에 연결하는 구성은 별도입니다. `DEMO_CLOUD_DIRECTORY`가 가리키는 디렉터리에 승인된 DB·Secret ARN·DMS 작업의 `config.json`을 준비합니다. Cloud SQL DNS와 IP, 실행 UID·GID는 활성 세션의 환경 파일 `runtime/active-cloud.env`에 둡니다. AWS 인증 파일과 GCP ADC는 읽기 전용으로 마운트합니다.

```sh
docker compose --env-file runtime/active-cloud.env \
  -f demo/cloud.compose.yaml up -d --build --wait
# http://127.0.0.1:5190
```

클라우드 구성은 모의 SDK와 로컬 TLS 우회를 적용하지 않습니다. DB·PgBouncer의 외부 연결은 `verify-full`로 검증합니다. 첫 화면의 `처음부터 시연 시작`은 앱 등록 → 계획·사전 검사 → 초기 복사·CDC → 동기화 확인 → 쓰기 정지 → Cloud SQL 전환 → 앱 재개를 순서대로 실행합니다. 새 세션은 앱 미등록·대상 업무 DB 없음·DMS NOT_STARTED에서 대기합니다. 시연을 준비하는 동안 초기 복사를 먼저 실행하지 않습니다.

원본 복귀 후에는 같은 전체 실행이 CDC 재개부터 이어갑니다. `원본 복귀·앱 재개`는 복귀와 재개를 함께 실행합니다. 각 단계의 개별 버튼도 유지합니다. 실행할 세션 디렉터리는 `DEMO_CLOUD_DIRECTORY`로 지정합니다. 이전 세션의 상태·설정·DB 데이터를 보존하고 새 세션을 준비할 수 있습니다.

CDC 중 정지한 DMS 작업은 데이터 초기화 없이 Resume합니다. 이미 승격·삭제·정리한 작업은 재사용하지 않습니다. 대상 쓰기를 시작한 뒤에는 역방향 CDC 없이 원본으로 복귀할 수 없습니다. 접수 결과가 불명확하면 전체 실행을 멈춥니다. 저장된 실행 ID가 있고 작업 진행 중임을 확인한 경우에만 그 결과를 조회하고 이어갑니다. 화면에서 클라우드 데이터 초기화·모의 장애 주입은 제공하지 않습니다. 이 Compose를 정지해도 클라우드 DB와 DMS는 자동 정지하지 않습니다.

아래는 외부 클라우드를 쓰지 않는 모의 구성입니다.

```sh
docker compose -f demo/compose.yaml up -d --build --wait
# http://127.0.0.1:5190
# 종료: docker compose -f demo/compose.yaml stop
```

화면에서 앱 등록, 복사, 쓰기 정지, 전환, 재개를 실행합니다. 전체 실행 또는 각 단계의 버튼을 누릅니다. 대상 쓰기 전 원본 복귀와 승격 응답 유실 복구도 시험할 수 있습니다. `초기화`는 이 시연의 DB 데이터와 관리 상태를 초기화합니다. 다른 랩·클라우드 리소스는 건드리지 않습니다.

PostgreSQL·PgBouncer·공통 API·Worker·DynamoDB는 실제로 동작합니다. **GCP DMS는 모의 SDK**이며 CDC 대신 조회 시 전체 스냅샷을 복사합니다. 실제 클라우드 DMS 연동·성능·무중단을 증명하는 시연은 아닙니다. 시연 서버에서만 로컬 Secret과 Docker 평문 연결을 허용하며, 운영 진입점에는 적용하지 않습니다. 고정 시연 비밀번호를 쓰므로 로컬에서만 사용하십시오. DB 포트는 Docker 내부에 있고, 대시보드와 PgBouncer만 `127.0.0.1`에 공개합니다.

### 기존 자체 복제 랩

로컬 Compose 프로젝트에 DynamoDB Local·원본 DB·대상 DB·PgBouncer를 만듭니다. 기존 Foxtrot 데모와 다른 볼륨·포트를 사용합니다. 포트와 비밀번호는 생성해 runtime/lab에 저장하며 Git에서 제외합니다.

```sh
make lab-up
RUN_DB_OPERATOR_INTEGRATION=1 uv run --locked pytest
```

실제 DB 전환, 대상 새 쓰기 후 역방향 롤백, RESUME 응답 유실, API 재시작, 실행기 중복 방지, 멱등 접수, 만료된 정지 근거 거절을 검사합니다. 테스트마다 새 업무 DB를 만들며 기존 DB를 초기화하지 않습니다.

로컬 설정으로 서버를 직접 실행할 수 있습니다.

```sh
uv run --locked python scripts/run_lab.py api --port 8080
uv run --locked python scripts/run_lab.py worker
```

시험 후 make lab-down으로 이번 프로젝트의 컨테이너만 정지합니다. 데이터는 보존합니다.

## API와 실행 흐름

| Method | Path | 역할 |
| --- | --- | --- |
| GET | /v1/capabilities | 지원 조건 조회 |
| POST | /v1/migrations | 계획 등록 |
| GET | /v1/migrations/{migrationId} | 실제 상태·진행 조건 조회 |
| POST | /v1/migrations/{migrationId}/actions | 명령 접수 |
| GET | /v1/operations/{operationId} | 실행 결과 조회 |
| POST | /v1/bindings | 앱의 DB 접속점 등록·기존 접속점 공유 |
| GET | /v1/bindings/{bindingId} | 등록 상태·앱 목록·접속점 조회 |
| POST | /v1/bindings/{bindingId}/actions | 해제·복원·실패 확인·재시도 |

동적 등록은 `DB_OPERATOR_BINDINGS_ENABLED=true`일 때 사용합니다. 기본 정적 Registry 방식도 유지합니다. 동적 모드에서는 플랫폼 백엔드가 내부 SDK로 DB·업무 범위를 먼저 게시하고, API는 그 참조만 받습니다. 설정은 Worker가 alias별로 적용하며 Task를 재시작하지 않습니다. 앱별 Secret은 실행 시 Secrets Manager에서 읽습니다. 최초 등록·전환·해제는 별도 작업입니다.

앱별 고정 실행 단위는 API·Worker·PgBouncer를 같은 Task에 유지합니다. 다음 직접 입력은 내부 참조와 alias를 서버에서 만듭니다. 앱마다 다른 `DB_OPERATOR_STATE_NAMESPACE`와 API 토큰을 사용합니다. `DB_OPERATOR_APP_RESOURCE_ID`를 설정하면 해당 리소스 ID만 허용합니다. DB Secret은 기존 `adminDsn`, `replicationDsn`, `writerPassword` 형식을 사용하며 API에는 Secret ARN만 보냅니다.

```json
POST /v1/bindings
{
  "resourceId": "database",
  "kind": "DATABASE",
  "source": {
    "host": "source.example", "port": 5432, "database": "appdb",
    "secretRef": "arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:app-source",
    "provider": "SELF_MANAGED", "writerRole": "app_writer"
  },
  "schema": {
    "tables": ["public.inventory", "public.orders"],
    "sequences": ["public.orders_id_seq"]
  },
  "workload": {"participants": ["app-web", "app-worker"]}
}
```

응답 `202`에는 `operationId`, `statusUrl`, `bindingId`, `bindingRef`, 고정 `proxy` 주소가 있습니다. 작업 성공과 binding의 `ACTIVE`를 확인한 뒤 연결합니다. `proxy`는 `host`, `port`, `database`, `protocol: postgresql`입니다. binding 조회의 `source`는 저장한 현재 경로의 DB이며 `lastMigrationId`는 마지막 이전 작업입니다.

```json
POST /v1/migrations
{
  "workflowId": "workflow-next",
  "resourceId": "database",
  "kind": "DATABASE",
  "source": {
    "host": "source.example", "port": 5432, "database": "appdb",
    "secretRef": "arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:app-source",
    "provider": "SELF_MANAGED", "writerRole": "app_writer"
  },
  "target": {
    "host": "target.example", "port": 5432, "database": "appdb",
    "secretRef": "arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:app-target",
    "provider": "SELF_MANAGED", "writerRole": "app_writer"
  },
  "policy": {"rollbackMode": "BEFORE_TARGET_WRITE", "maxQuiesceSeconds": 300}
}
```

직접 입력에서 `bindingRef`와 `workloadScopeRef`를 생략하면 앱의 유일한 접속점과 등록 범위를 선택합니다. `profile` 기본값은 현재 원본·대상 조합으로 선택합니다. 기존 `sourceRef`·`targetRef` 요청과 action API의 revision·멱등 키는 유지합니다. 관리형 DB에는 `providerInstance`도 필요합니다. 자체 관리 PostgreSQL 18 쌍과 관리형 PostgreSQL 17(RDS, Cloud SQL) 사이의 양방향을 지원합니다. 대상 쓰기 후 롤백은 등록 단계에서 거절합니다.

`FINALIZE`는 해당 계획의 복제 객체와 내부 barrier만 정리합니다. 업무 데이터와 고정 프록시·Worker는 유지합니다. 이후 현재 활성 DB를 다음 `source`로 새 이전을 등록할 수 있습니다. 이전 계획이 미완료·UNKNOWN이거나 현재 경로와 `source`가 다르면 차단합니다. 새 대상의 schema·권한 준비와 빈 업무 table 조건은 그대로 적용합니다. 대상 데이터를 자동으로 비우지 않습니다. 같은 앱의 다음 업무 정지에는 증가한 generation을 사용합니다. 완료한 직접 입력 계획은 조회와 같은 멱등 요청의 재전송만 허용합니다. `RESET`은 기존 로컬 시연 전용이며 실제 클라우드에서 차단합니다.

```text
PREPARE → START_SYNC → READY
  → QUIESCE → CUTOVER → RESUME TARGET
  → 앱 업무 재개·검사 → readiness.advance=PASS
  → 오케스트레이터의 FINALIZE
```

CUTOVER와 RESUME은 별도 명령입니다. 202는 접수 성공이며 실행 성공이 아닙니다. 앱을 멈추고 재개하는 책임은 오케스트레이터에 있습니다. 연결 방법과 quiesceRef 예제는 [오케스트레이터 가이드](docs/orchestrator.md)에 있습니다.

## 지원 제약

DMS 경로의 조건은 [DMS API 가이드](docs/dms-api.md)를 따릅니다. 아래는 기존 자체 복제 랩 프로필의 조건입니다.

- PostgreSQL 18, transaction pooling, 같은 schema, PK와 DEFAULT replica identity가 있는 일반 table, 증가량 1·cache 1·비순환 sequence를 사용합니다.
- 업무 DB 전체 table·sequence와 모든 앱·Worker·예약 작업·우회 접속을 등록합니다. DDL·사용자 trigger·RLS·파티션·large object·2PC는 지원하지 않습니다.
- 업무 LOGIN 역할은 이 DB 전용이어야 하며 관리자·상속 역할을 사용할 수 없습니다. (release/1: table 소유는 허용) 작업 상태는 별도 DynamoDB에 저장합니다.
- 실행기는 하나입니다. 잠금을 잃으면 다음 외부 변경을 막습니다. 이전 원격 연결이 남으면 RECONCILE도 복구 완료로 판정하지 않습니다.
- BEFORE_TARGET_WRITE는 대상 쓰기를 열기 전까지만 직접 복귀를 제공합니다. REVERSE_REQUIRED는 역복제를 확인해야 대상 쓰기를 엽니다. 최신성을 확인하지 못하면 자동 복귀하지 않습니다.
- FINALIZE는 계획의 publication·subscription·slot만 정리합니다. 원본 DB·업무 table·대상 데이터를 삭제하지 않습니다.

## 검사와 코드

```sh
make check
uv build
```

```text
src/db_operator/
├─ api.py          HTTP 계약·인증
├─ service.py      요청 처리·진행 조건
├─ worker.py       명령 실행·실패 복구
├─ postgres.py     DB 관측·스키마·복제·PgBouncer 제어
├─ dms.py          DMS SDK·실행 기록·CLI
├─ runtime.py      ECS 설정·프록시 복원·역할별 실행
├─ store.py        DynamoDB 트랜잭션·잠금·명령·프록시 경로
├─ control.py      업무 정지 근거·오케스트레이터 확인 기록
├─ config_file.py  설정 파일 권한 보존·원자적 교체
├─ config.py       리소스 등록·설정
├─ models.py       요청 형식·오류
├─ cli.py          기본 실행 명령
├─ __init__.py     패키지 선언
└─ contract.json   공통 OpenAPI 계약
```

[공통 Design Doc](https://app.notion.com/p/40d8bee9ada4815e9e94ee454be82d4a) · [DB 설계](https://app.notion.com/p/40c8bee9ada481b0b268cee206e3e51f)

구현 근거: [PostgreSQL subscription·origin](https://www.postgresql.org/docs/18/sql-createsubscription.html), [복제 origin 내구성](https://www.postgresql.org/docs/18/functions-admin.html), [PgBouncer 제어 명령](https://www.pgbouncer.org/usage.html), [psycopg 단순 query cursor](https://www.psycopg.org/psycopg3/docs/advanced/cursors.html).

## 마이그레이션 관측

추가 필드와 연결 방법은 [관측 연결](docs/migration-observability.md)을 참고하세요.
