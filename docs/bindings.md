# 앱 DB 접속 등록·해제

PgBouncer 하나에 DB별 alias를 만듭니다. 앱은 발급된 접속점을 상시 사용합니다. 등록·해제는 마이그레이션 계획과 별도 작업입니다.

## 준비

ECS 설정의 `bindings_enabled=true`, `public_host`를 설정합니다. `DB_OPERATOR_REGISTRY_JSON`은 빈 Registry입니다. 기본 Secret의 `DB_OPERATOR_RUNTIME_USERS_JSON`에는 PgBouncer 관리 계정만 넣습니다. 앱별 접속 정보는 Secrets Manager에서 읽습니다.

플랫폼 백엔드가 내부 SDK로 DB와 업무 범위를 먼저 게시합니다. Operator의 HTTP API는 임의 DB 주소나 Secret 값을 받지 않습니다.

```python
from db_operator.config import Database, ResourceCatalog
from db_operator.store import Store

catalog = ResourceCatalog(Store(state_table, namespace=state_namespace, region="ap-northeast-2"))
catalog.database(
    "db:reservation",
    Database(
        identity="cluster-01/reservation",
        route_host="db-source.example.com",
        database="reservation",
        writer_role="reservation_writer",
        secret_ref="arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:reservation-REPLACE",
    ),
    tables=["public.inventory", "public.orders"],
    sequences=["public.orders_id_seq"],
)
catalog.workload("scope:reservation", ["reservation-api", "reservation-worker"])
```

DB Secret의 JSON 키는 `adminDsn`, `replicationDsn`, `writerPassword`입니다. 관리·복제 DSN은 공인망에서 `sslmode=verify-full`과 검증용 CA를 사용합니다. 초기 등록에는 운영할 DB 하나만 필요합니다. 이전 대상은 마이그레이션 전에 별도 참조로 게시합니다.

리소스 정의는 처음 게시한 값으로 고정합니다. 정의를 바꾸려면 새 참조를 사용합니다. Secret 값과 비밀번호는 DynamoDB·API 응답·작업 로그에 저장하지 않습니다. 각 binding은 다른 writer 역할을 사용합니다.

## 등록

```http
POST /v1/bindings
Authorization: Bearer TOKEN
Idempotency-Key: register-reservation-001
Content-Type: application/json
```

```json
{
  "appRef": "app:reservation",
  "databaseRef": "db:reservation",
  "alias": "reservation",
  "workloadScopeRef": "scope:reservation"
}
```

`202` 응답의 `statusUrl`로 작업 결과를 조회합니다. `SUCCEEDED` 후 `GET /v1/bindings/{bindingId}`의 `status=ACTIVE`와 `endpoint`를 확인합니다. 완료 전에는 endpoint를 발급하지 않습니다.

Worker는 차단된 alias를 만들고 계정·경로를 설정합니다. 실제 앱 계정으로 접속해 DB를 확인한 뒤 등록을 완료합니다. 앱에 host·port·database와 별도의 업무 Secret을 주입합니다. 이 API는 DB·계정을 생성하거나 앱을 배포하지 않습니다.

같은 DB를 공유하는 앱은 같은 alias와 현재 `databaseRef`로 등록합니다. 기존 binding에 앱을 추가하고 정지 확인 대상을 합칩니다. 같은 DB를 별도 alias로 중복 등록하거나 여러 binding에 같은 writer 이름을 쓰면 거절합니다.

## 해제

오케스트레이터가 실제 앱 정지 또는 직접 DB 접속 전환을 확인한 뒤 근거를 발급합니다. 아래 SDK는 확인 결과를 기록하며 앱을 멈추지 않습니다.

```python
from db_operator.control import ControlPlaneRecords, actor_ack

records = ControlPlaneRecords(store)
evidence_ref = records.binding_release(
    binding_id,
    "app:reservation",
    mode="STOP_APP",
    participants=[
        actor_ack("reservation-api", 17, "HELD", in_flight=0),
        actor_ack("reservation-worker", 17, "HELD", in_flight=0),
    ],
    connections_closed=True,  # 실제 앱 연결 풀 종료를 확인한 뒤 기록합니다.
)
```

근거 저장 후 GET으로 최신 binding revision을 읽고 요청합니다.

```http
POST /v1/bindings/{bindingId}/actions
Authorization: Bearer TOKEN
Idempotency-Key: unregister-reservation-001
Content-Type: application/json
```

```json
{
  "action": "UNREGISTER",
  "expectedRevision": 4,
  "appRef": "app:reservation",
  "mode": "STOP_APP",
  "evidenceRef": "binding-release:SDK가-발급한-값"
}
```

`4`는 예시입니다. `STOP_APP`은 모든 참여자의 HELD·진행 중 업무 0·앱 연결 풀 종료 확인이 필요합니다. `KEEP_RUNNING`은 현재 운영 DB로 직접 연결한 뒤 모든 참여자의 RUNNING, `direct_database_ref`, `probe_result="PASS"`, 실제 업무 검사 `probe_ref`도 기록합니다. 공유 DB의 일부 앱만 KEEP_RUNNING으로 해제하는 요청은 거절합니다.

해제 근거는 30초 동안 유효합니다. 대기 중에는 `renew_binding_release`로 실제 확인 결과를 갱신합니다. Worker는 변경 전마다 근거를 다시 검사합니다.

플랫폼은 앱의 새 인스턴스도 해제 상태를 지키게 해야 합니다. STOP_APP 확인 후 앱·Worker·예약 작업이 다시 시작되지 않게 제어합니다. 마이그레이션 계획을 새로 만들면 이전 해제 근거는 무효입니다. 완료된 이전 기록은 이후 DB·alias가 해제돼도 원격 접속 없이 조회합니다.

마지막 앱의 해제 순서는 신규 연결 차단 → 기존 연결 0 확인 → PAUSE → 서버 연결 0 확인 → 전용 인증·alias 제거 → RELOAD → 실제 제거 확인입니다. 연결이 남으면 시간 상한 후 멈추며 강제로 끊지 않습니다. 기본 상한은 10초입니다.

다른 앱이 같은 binding을 사용하면 앱 연결 관계만 해제합니다. alias와 다른 앱의 연결은 유지합니다. 마지막 앱의 해제 후에도 DB·데이터·DB 계정은 유지합니다. UNREGISTERED 기록은 남기며 재시작 때 alias를 복원하지 않습니다.

## 실패와 복원

| 명령·상황 | 동작 |
|---|---|
| 이전 작업이 남아 있음 | 등록 변경·해제 거절. CANCEL 상태도 FINALIZE 후 해제 |
| 응답 유실 | 같은 키와 같은 본문으로 접수 결과 확인 |
| UNKNOWN | RECONCILE로 실제 alias·경로·연결을 대조. 변경을 자동 재실행하지 않음 |
| RECONCILE이 닫힌 중간 상태를 확인 | binding은 FAILED. 최신 해제 근거로 RETRY |
| FAILED 등록 철회 | CANCEL. 실제 차단을 확인하고 자원 점유를 반환 |
| RESTORE | 현재 저장 경로·인증을 다시 적용. 진행 중인 이전 명령과 경합 금지. HELD·UNKNOWN 이전은 재개하지 않음 |
| Task 재시작 | ACTIVE binding을 각각 복원. 한 binding의 복원 실패는 해당 alias를 차단하고 FAILED로 기록 |

재시도는 새 Idempotency-Key와 최신 revision을 사용합니다. 진행 중인 이전의 업무 범위는 binding 응답의 `workloadScopeRef`를 사용합니다. 마이그레이션 완료 후에도 접속점은 유지합니다. FINALIZE는 복제 객체만 정리합니다.

## 권한과 검증 범위

상태·잠금은 같은 DynamoDB namespace를 사용합니다. `CATALOG_DB`, `CATALOG_SCOPE`, `BINDING_GATE`는 별도 `<namespace>:control` 영역입니다. 플랫폼 백엔드만 이 영역을 쓸 수 있어야 합니다. API·Worker는 읽기만 허용합니다. 앱이 이 SDK나 Operator 서비스 토큰을 사용하면 안 됩니다.

Task Role에 앱 DB Secret의 `secretsmanager:GetSecretValue`를 허용합니다. 고객 관리 KMS 키를 쓰면 해당 키의 `kms:Decrypt`도 필요합니다. 허용할 Secret ARN을 제한합니다. ECS 실행 역할의 기본 Secret 주입 권한과 구분합니다. [GetSecretValue 권한](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html)

단일 PgBouncer·단일 Task 구성입니다. Task 교체는 모든 앱의 연결을 끊습니다. 공개 DNS·허용 IP 갱신과 앱의 실제 정지·배포는 플랫폼이 처리해야 합니다. 로컬 자체 운영 PostgreSQL 18·PgBouncer 검증과 실제 ECS·IAM·Secrets Manager 검증은 구분합니다. 기존 정적 Registry의 자동 가져오기는 제공하지 않습니다.

현재 등록 검사는 실제 DB 중복을 막기 위해 `pg_control_system()` 조회 권한도 사용합니다. RDS·Cloud SQL 등록·이전 지원을 검증한 결과로 해석하면 안 됩니다.
