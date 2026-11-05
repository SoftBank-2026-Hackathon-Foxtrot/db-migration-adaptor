# db-migration-adaptor

PostgreSQL 논리 복제와 PgBouncer를 제어하는 Foxtrot DB Operator입니다. FastAPI는 명령을 DynamoDB에 저장하고, Worker가 실제 이전 작업을 실행합니다.

[구현 설명](docs/implementation.md) · [구조·전환·롤백 도식](docs/architecture/index.html)

[장애 경로 검토와 수정](docs/review-fixes.md): 설정 보존, 조회 장애, 직접 복귀, LSN 검증, 상태 경합을 보강했습니다.

[ECS 배포 구성](deploy/ecs/README.md): 단일 Task 안의 backend(API·Worker)와 PgBouncer, 경로·정지 상태 복원과 배포 JSON 생성 방법입니다.

[동작·성능 검증 리포트](docs/performance-report.md): 로컬 처리량, 전환·롤백 시간, 발견한 문제와 적용 범위입니다.

```text
오케스트레이터 → API → DynamoDB → DB Worker
                                      ├ 원본·대상 PostgreSQL
앱·업무 Worker → 고정 PgBouncer 주소     └ PgBouncer 관리·설정
```

## 구현 범위

- 초기 전체 복사와 CDC: publication·subscription.
- 전환: 정지 근거 검사 → PAUSE → writer 차단 → barrier 적용·내구성 확인 → 복제 정지 → 데이터·시퀀스 대조 → 경로 변경.
- 역방향 복제: 대상 쓰기 전에 copy_data=false·origin=none으로 준비. 최종 역적용과 검증 후 원본 복귀.
- 복구: 실행 의도 선기록, 멱등 요청, revision 검사, 전환 ID 고정, UNKNOWN 차단과 RECONCILE.
- 진행 판정: 앱 재개·업무 검사·후속 작업과 30초 안정 구간 확인.

현재 검증 범위는 **PostgreSQL 18 자체 운영 + PgBouncer 1.25.1의 로컬 통합 환경**입니다. RDS·Cloud SQL·실제 온프레미스 네트워크는 검증하지 않았습니다. 전환 중 쓰기에는 정지 구간이 있습니다.

## 실행

Python 3.13과 uv를 사용합니다.

```sh
uv sync --locked --dev
```

실제 환경은 registry.example.json과 .env.example을 기준으로 등록합니다. 대상 schema·앱 역할·복제 권한은 미리 준비해야 합니다. 대상 업무 table은 비어 있어야 하고 대상 업무 계정은 NOLOGIN이어야 합니다. 환경 Secret을 주입한 뒤 다음 명령을 각각 실행합니다.

```sh
uv run --locked foxtrot-db init
uv run --locked foxtrot-db api --host 127.0.0.1 --port 8080
uv run --locked foxtrot-db worker
```

API 문서는 /docs, /redoc, /openapi.json입니다. 서비스 API에는 Authorization: Bearer 토큰이 필요합니다. 기본 롤백 보존 기간은 3600초입니다.

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

```text
PREPARE → START_SYNC → READY
  → QUIESCE → CUTOVER → RESUME TARGET
  → 앱 업무 재개·검사 → readiness.advance=PASS
  → 보존 기간 종료 → FINALIZE
```

CUTOVER와 RESUME은 별도 명령입니다. 202는 접수 성공이며 실행 성공이 아닙니다. 앱을 멈추고 재개하는 책임은 오케스트레이터에 있습니다. 연결 방법과 quiesceRef 예제는 [오케스트레이터 가이드](docs/orchestrator.md)에 있습니다.

## 지원 제약

- PostgreSQL 18, transaction pooling, 같은 schema, PK와 DEFAULT replica identity가 있는 일반 table, 증가량 1·cache 1·비순환 sequence를 사용합니다.
- 업무 DB 전체 table·sequence와 모든 앱·Worker·예약 작업·우회 접속을 등록합니다. DDL·사용자 trigger·RLS·파티션·large object·2PC는 지원하지 않습니다.
- 업무 LOGIN 역할은 이 DB 전용이어야 하며 소유자·관리자·상속 역할을 사용할 수 없습니다. 작업 상태는 별도 DynamoDB에 저장합니다.
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
