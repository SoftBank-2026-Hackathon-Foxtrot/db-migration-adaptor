# 오케스트레이터 연결

운영 설계는 [GCP DMS 공통 API 경로](dms-api.md)를 사용합니다. 공통 명령·정지 근거는 유지합니다. 아래 역복제 설명은 기존 자체 복제 프로필에만 적용합니다. DMS 프로필은 대상 쓰기 허용 전 롤백만 제공하며 `REVERSE_REQUIRED`를 거절합니다.

DB Operator는 앱 정지를 수행하지 않습니다. 오케스트레이터가 실제 정지·재개를 제어하고, DynamoDB에 확인 결과를 기록합니다. 이 기록을 만드는 HTTP API는 제공하지 않습니다.

최초 앱 등록·해제는 [binding 가이드](bindings.md)를 따릅니다. 동적 모드의 이전 계획에는 ACTIVE binding의 ID, 현재 databaseRef, binding 전체 앱을 포함한 workloadScopeRef를 사용합니다. 정지 확인 대상에는 같은 DB를 공유하는 모든 앱이 포함됩니다.

## 실행 순서

1. GET /v1/capabilities로 지원 조건을 확인합니다.
2. POST /v1/migrations로 등록한 sourceRef·targetRef·bindingRef·workloadScopeRef를 전달합니다.
3. PREPARE → START_SYNC 후 GET 상태가 READY인지 확인합니다. START_SYNC의 SUCCEEDED는 초기 복사 완료가 아닙니다.
4. 앱·Worker·예약 작업의 신규 업무를 막고, 진행 중 업무가 0건인지 확인합니다. 전체 실행 주체를 등록하지 못하면 진행하지 않습니다.
5. 현재 transitionId·generation·topologyRevision에 맞는 HELD 기록을 저장합니다. 그다음 GET으로 최신 revision을 읽고, 반환된 quiesceRef로 QUIESCE를 요청합니다.
6. QUIESCE 완료 후 정지 상태를 유지하고 새 topologyRevision의 quiesceRef를 발급합니다. CUTOVER를 요청합니다.
7. CUTOVER 완료 후 새 버전의 정지 근거를 발급하고 RESUME TARGET을 요청합니다.
8. 리소스 재개가 확인되면 오케스트레이터가 앱 업무를 재개합니다. 실제 앱 경로의 업무 검사를 수행하고 RUNNING 확인과 검사 근거를 저장합니다.
9. 10초 이하 간격으로 관측을 갱신합니다. readiness.advance=PASS와 전체 의존 관계가 모두 확인돼야 다음 리소스의 정지를 시작합니다.
10. 오케스트레이터가 되돌릴 필요가 없다고 판단하면 FINALIZE합니다. 어댑터는 대기 시간 없이 상태만 확인합니다. 원본 DB는 삭제하지 않습니다.

HELD·RUNNING 제어 기록이 바뀌면 revision과 진행 PASS도 바뀔 수 있습니다. 제어 기록 저장 → GET으로 최신 revision 확인 → 명령 접수 순서를 지킵니다. 매 명령은 최신 revision과 새 Idempotency-Key를 사용합니다. 같은 요청의 재전송은 최초 키·본문을 그대로 사용합니다. 202 응답의 Location으로 실행 결과를 조회합니다.

GET은 기본 2초 동안 관측 결과를 재사용합니다. DB_OPERATOR_OBSERVATION_CACHE_SECONDS로 0~10초 사이에서 설정합니다. 관측 실패만으로 명령 결과를 UNKNOWN으로 바꾸지는 않습니다. 진행 조건이 OBSERVATION_UNAVAILABLE이면 다음 GET으로 다시 확인합니다. 실제 명령이 UNKNOWN으로 끝났다면 기존대로 RECONCILE이 필요합니다.

## 정지 확인 기록

상태 테이블의 키는 namespace와 record입니다. ControlPlaneRecords는 같은 상태 영역에 기록하는 서버 내부 SDK입니다.

```python
from db_operator.control import ControlPlaneRecords, actor_ack
from db_operator.store import Store, now

records = ControlPlaneRecords(
    Store(state_table, namespace=state_namespace, region="ap-northeast-2")
)
held_since = now()  # 실제 업무 정지를 시작한 시각. 갱신할 때 유지합니다.

# 실제 제어 채널에서 모든 인스턴스의 정지 확인을 받은 뒤 호출합니다.
ref = records.held(
    migration_id=migration_id,
    transition_id=transition_id,
    generation=17,
    held_since=held_since,
    participants=[
        actor_ack("reservation-api", 17, "HELD", in_flight=0),
        actor_ack("reservation-worker", 17, "HELD", in_flight=0),
        actor_ack("reservation-scheduler", 17, "HELD", in_flight=0),
    ],
)
```

actor_ack는 직렬화 함수입니다. 이 함수를 호출했다고 앱이 멈추지는 않습니다. 실제 제어 채널의 확인을 먼저 받아야 합니다. 신규 인스턴스도 저장된 HELD를 확인한 뒤 시작하도록 앱 측 제어를 구현해야 합니다.

같은 ref의 확인은 records.renew로 10초 이하 간격으로 갱신합니다. 근거는 30초 후 만료하지만 업무 정지는 유지합니다. roles·프록시 경로가 바뀌면 Operator가 topologyRevision을 올립니다. 현재 HELD를 확인한 뒤 records.held로 새 버전의 ref를 발급합니다. 정지가 이어졌다면 generation과 held_since는 유지합니다. 재개 후 다시 정지하면 generation을 증가시킵니다.

RESUME 성공 후 오케스트레이터가 실제 업무를 열고 확인했다면 다음과 같이 기록합니다.

```python
records.running(
    migration_id,
    generation=17,
    participants=running_acknowledgements,
    probe_result="PASS",
    probe_ref="evidence:reservation-business-check-001",
)
```

업무 검사는 앱이 실제로 사용하는 경로로 읽기·쓰기·정합성·시험 데이터 정리를 확인해야 합니다. Operator의 DB 연결 검사만으로 대신하지 않습니다. 참여자 확인과 probe 근거는 30초 이내의 최신 기록이어야 합니다. 30초 안정 구간의 관측이 10초 넘게 끊기면 처음부터 다시 측정합니다. generation·topology·업무 상태가 바뀌거나 참여자·probe 확인이 실패해도 다시 측정합니다. 같은 generation의 정상 heartbeat와 처리 중 요청 수 변화는 구간을 유지합니다.

## 롤백

- 대상 쓰기 전: QUIESCE 완료 상태 → ROLLBACK → 새 버전의 정지 근거 → RESUME SOURCE.
- 최종 동기화 중 대상 장애: 원본·프록시의 정지가 확인되고 대상 쓰기 허용 기록이 없으면 RECONCILE → ROLLBACK → RESUME SOURCE. 원본에서 계획의 정방향 복제를 차단하고 원본 schema·데이터만 검증합니다. 대상 재연결 후 FINALIZE합니다.
- 대상 쓰기 후: 같은 transitionId로 업무를 다시 정지하고 generation 증가 → QUIESCE로 최종 역복제·데이터·시퀀스 검증 → ROLLBACK → 새 버전의 정지 근거 → RESUME SOURCE.
- 대상 재기동 후에는 역복제가 다시 연결됐는지 먼저 확인합니다. 역복제가 준비되지 않은 상태에서 요청한 QUIESCE는 진행하지 못할 수 있습니다. UNKNOWN으로 끝나면 RECONCILE 후 최신 정지 근거로 다시 요청합니다.
- BEFORE_TARGET_WRITE 모드에서 대상 쓰기를 허용한 뒤에는 빠른 롤백을 지원하지 않습니다.
- PAUSE 뒤 원본 차단 전 중단: RECONCILE → 새 정지 근거 → QUIESCE로 차단을 완료합니다. 기한 초과나 미사용 대상 장애 시에는 원본 복귀만 허용합니다.
- LOGIN 뒤 프록시 RESUME 전 중단: RECONCILE → 새 정지 근거 → 같은 위치의 RESUME을 요청합니다. 대상 쓰기 허용 기록은 지우지 않습니다.
- FINALIZE 중단: RECONCILE → FINALIZE로 남은 정리를 이어갑니다. 승인 이후 같은 scope의 새 제어 변경은 거절합니다.
- UNKNOWN이면 실제 상태를 확인하는 RECONCILE만 요청합니다. 어댑터가 실제 상태와 이전 원격 실행 종료를 확인할 때까지 다른 변경을 요청하지 않습니다.
- 원본 업무를 재개한 계획은 새 전환 ID로 다시 열지 않습니다. FINALIZE 후 새 계획을 등록합니다. 기존 대상 데이터는 임의로 삭제하지 않습니다.

정지 시간 상한을 넘으면 새 대상 전환과 대상 재개를 거절합니다. 갱신한 정지 근거가 있으면 원본 복귀·원본 재개는 요청할 수 있습니다. 대상 쓰기 전에는 원본이 계속 기준 DB이므로 불완전한 대상 복사를 기다리지 않고 원본을 검증해 복귀할 수 있습니다. 대상 쓰기 후에는 역방향 QUIESCE를 다시 수행해 최종 적용·데이터·시퀀스를 확인해야 합니다. 시간 초과만으로 자동 재개하지 않습니다.

## 권한과 배치

- API는 별도 Worker와 같은 registry·DynamoDB를 사용합니다. 변경 실행기는 하나만 실행합니다. DynamoDB의 조건부 트랜잭션과 소유자 잠금으로 중복 실행을 막습니다.
- 오케스트레이터만 quiesce_gates·workload_observations를 쓸 수 있게 DB 권한을 나눕니다. Operator의 HTTP API는 이 기록을 수정하지 않습니다. init은 관리용 설치 계정으로 실행합니다.
- API의 서비스 토큰은 환경 Secret으로 주입합니다. 외부 배치는 사설망과 TLS 진입점 뒤에서 수행합니다. 기본 API listen 주소는 127.0.0.1입니다.
- PostgreSQL 관리·복제·업무 계정을 분리합니다. 이 첫 프로필은 관리 SQL에 superuser를 요구하므로 관리형 DB를 지원으로 선언하지 않습니다.
- Worker는 PgBouncer와 같은 호스트 또는 같은 설정 디렉터리 볼륨을 사용합니다. 파일 하나를 bind mount하면 원자적 교체가 반영되지 않을 수 있으므로 디렉터리를 공유합니다.
- 설정 교체 권한으로 기존 UID·GID·mode·확장 속성을 보존할 수 있어야 합니다. 확장 ACL은 지원하지 않으며 PREPARE에서 거절합니다. PgBouncer 사용자의 그룹 읽기 권한을 유지해야 합니다.
- config_path는 alias가 직접 정의된 파일을 가리켜야 합니다. 경로 값만 수정하며 나머지 옵션·주석·%include는 보존합니다. include로 읽는 alias 파일도 등록할 수 있습니다. include만 있는 상위 파일을 등록하면 PREPARE에서 거절합니다.
- registry에는 Secret 값 대신 환경 변수 이름을 등록합니다. 복제 DSN은 상대 DB가 접속할 수 있는 주소여야 합니다. PgBouncer의 route_host와 관리 연결 주소는 다를 수 있습니다.
- 외부 DDL·미등록 writer·관리 작업은 이전 동안 금지합니다. 이 제어를 보장할 수 없는 DB는 이 프로필에 등록하지 않습니다.

실행 의도와 확인 결과는 DynamoDB의 `OP#` 항목에 저장합니다. 현재 상태와 데이터 digest·barrier·검증 근거는 `MIG#` 항목의 `state`·`internal`에 저장합니다. API는 비밀번호·복제 DSN·관리 DSN을 반환하지 않습니다. 불명으로 끝난 명령의 과거 결과는 보존하고 RECONCILE을 별도 명령으로 남깁니다.
