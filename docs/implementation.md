# DB Operator 구현 구조

FastAPI는 명령을 별도 관리 PostgreSQL에 저장합니다. Python Worker가 저장한 명령을 읽고, PostgreSQL 논리 복제와 PgBouncer를 제어합니다.

[구조·전환·롤백·복구 도식 보기](architecture/index.html)

![DB Operator 전체 구조](architecture/architecture.png)

현재 지원 대상은 자체 운영 PostgreSQL 18과 PgBouncer입니다. RDS·Cloud SQL은 지원하지 않습니다. 전환에는 업무 쓰기 정지 구간이 있습니다.

## 1. 구성 요소

| 코드 | 역할 |
| --- | --- |
| [api.py](../src/db_operator/api.py) | Bearer 인증, 5개 공통 API, 명령 접수와 상태 조회 |
| [service.py](../src/db_operator/service.py) | 지원 조건, revision, 허용 명령, 다음 리소스 진행 조건 검사 |
| [store.py](../src/db_operator/store.py) | 계획·작업·멱등 키·실행 의도·검증 근거를 관리 DB에 저장 |
| [worker.py](../src/db_operator/worker.py) | 한 실행기만 허용하고 복사·전환·복귀 작업 실행 |
| [postgres.py](../src/db_operator/postgres.py) | PostgreSQL 복제·writer 권한·데이터 검사와 PgBouncer 제어 |
| [control.py](../src/db_operator/control.py) | 오케스트레이터의 HELD·RUNNING 확인 기록 SDK |
| [config.py](../src/db_operator/config.py) | 등록한 DB·프록시·업무 범위·Secret 환경 변수 이름 정의 |

앱은 같은 PgBouncer 주소를 계속 사용합니다. Worker가 PgBouncer의 연결 대상을 바꿉니다. 앱에 새 DB 주소를 전달하는 방식은 아닙니다.

Worker는 PgBouncer 설정 디렉터리를 공유해야 합니다. 설정을 원자적으로 교체하고 RELOAD를 실행합니다. API에 원격 호스트 정보만 전달한다고 배포까지 처리하지는 않습니다.

## 2. 요청 접수와 실제 실행

1. 오케스트레이터가 등록한 리소스 참조로 이전 계획을 만듭니다.
2. 각 명령에 최신 expectedRevision과 Idempotency-Key를 전달합니다.
3. API는 한 트랜잭션에서 상태 검사, QUEUED 작업 저장, 멱등 응답 저장을 수행합니다.
4. Worker가 관리 DB 잠금을 확보하고 작업을 획득합니다.
5. 외부 변경 전에 실행 의도를 먼저 저장합니다. 외부 변경 뒤에는 실제 상태와 결과를 저장합니다.
6. 오케스트레이터는 operationId로 완료 여부를 조회합니다. HTTP 202는 접수 성공입니다.

같은 키·본문은 최초 작업을 반환합니다. 같은 키에 다른 본문을 보내면 거절합니다. 미등록 호스트·SQL·셸 명령은 API로 받지 않습니다.

## 3. 정방향 전환

| 단계 | 실제 처리와 완료 조건 |
| --- | --- |
| PREPARE | PostgreSQL 18·logical WAL·권한·schema·등록 범위 검사. 대상 업무 table은 비어 있고 대상 writer는 NOLOGIN이어야 함 |
| START_SYNC | 원본 publication과 대상 subscription 생성. copy_data=true로 초기 복사 후 CDC 수행 |
| READY 확인 | 상태 조회로 초기 복제 준비를 확인. START_SYNC 명령 성공과 초기 복사 완료는 별개 |
| 앱 정지 | 오케스트레이터가 앱·업무 Worker·예약 작업을 정지. 모든 등록 주체의 HELD와 inFlight=0을 확인해 quiesceRef 저장 |
| QUIESCE | 근거 검사 → PgBouncer PAUSE → 업무 writer NOLOGIN → 기존 세션 0 확인 → 최종 barrier 적용·내구성 확인 → 복제 정지 → sequence 동기화·전체 데이터 digest 대조 |
| CUTOVER | REVERSE_REQUIRED 모드라면 대상 쓰기 전에 역복제 준비. PgBouncer 설정을 대상으로 변경·RELOAD. PAUSE와 writer 차단은 유지 |
| RESUME TARGET | 대상 쓰기 가능성을 관리 DB에 먼저 기록. 대상 writer LOGIN·PgBouncer RESUME 후 실제 앱 접속점의 DB·서버·계정 확인 |
| 앱 재개 | 오케스트레이터가 실제 업무를 재개하고 업무 검사·RUNNING 근거 기록 |
| 다음 리소스 | 상태·경로·업무 검사·후속 복제·30초 안정 구간을 확인. 관측 간격은 10초 이하. readiness.advance=PASS여야 진행 |
| FINALIZE | 보존 기간과 진행 조건 확인 후 이 계획의 publication·subscription·slot 정리. 원본 DB·업무 table·데이터는 삭제하지 않음 |

권한·경로가 바뀌면 topologyRevision도 바뀝니다. QUIESCE·CUTOVER·RESUME 사이에서 최신 HELD 근거를 다시 발급합니다. 같은 정지가 이어지면 generation과 최초 held_since는 유지합니다. 근거가 만료돼도 업무를 자동 재개하지 않습니다.

## 4. 롤백

**대상 쓰기 전:** 원본이 기준 DB입니다. writer 차단을 확인하고 원본 schema·데이터를 검사합니다. ROLLBACK으로 원본 경로를 복구합니다. 별도 RESUME SOURCE 후 오케스트레이터가 업무를 재개합니다. 불완전한 미사용 대상 복사가 직접 복귀를 막지는 않습니다.

**대상 쓰기 후:** REVERSE_REQUIRED 모드에서 미리 준비한 역복제가 필요합니다. 같은 transitionId로 업무를 다시 정지하고 generation을 증가시킵니다. QUIESCE가 대상 → 원본 최종 barrier·데이터·sequence를 검증합니다. 그다음 ROLLBACK → 최신 정지 근거 → RESUME SOURCE 순서로 복귀합니다.

BEFORE_TARGET_WRITE 모드에서 대상 쓰기를 열었다면 빠른 원본 복귀를 지원하지 않습니다. 역복제·최신성 근거가 부족하면 중단합니다. 자동 원본 복귀는 하지 않습니다.

## 5. 실패 후 복구

| 상황 | 처리 |
| --- | --- |
| 외부 변경 전 실패 | FAILED 기록. 실제 상태 대조 후 다음 명령 결정 |
| 외부 실행 의도 저장 후 실패·응답 유실 | UNKNOWN 기록. 대상 쓰기 가능성과 기존 실행 기록 보존. 다른 변경 차단 |
| Worker 재시작 | 중단한 RUNNING 작업을 자동 재실행하지 않음. 실행 의도에 따라 FAILED 또는 UNKNOWN 처리 |
| RECONCILE | 이전 원격 연결 종료, 실제 경로·권한·PAUSE·저장 근거 대조. 외부 자원은 변경하지 않음 |
| 실제 상태가 여전히 불명 | 계속 중단. allowedActions와 최신 상태를 확인하기 전에는 전환·재개·복귀하지 않음 |

Worker는 외부 변경 전마다 관리 DB 잠금을 확인합니다. 이전 원격 관리 연결이 idle이어도 남아 있으면 복구 완료 판정을 막습니다. 과거 UNKNOWN 작업의 결과를 성공으로 덮어쓰지 않고 별도 RECONCILE 작업을 남깁니다.

## 6. 현재 한계

- 자체 운영 PostgreSQL 18의 제한된 프로필입니다. 관리 SQL에 superuser가 필요합니다. RDS·Cloud SQL 권한 모델은 구현하지 않았습니다.
- 같은 schema, PK가 있는 일반 table, 등록한 모든 table·sequence·업무 주체가 필요합니다. DDL·RLS·partition·사용자 trigger·large object·2PC는 지원하지 않습니다.
- 인프라 생성, 앱의 실제 정지·재개 채널, Secret 배포, 네트워크 구성은 별도 구현입니다.
- 전체 데이터 digest 대조 시간은 DB 크기에 따라 늘어납니다. 큰 DB의 쓰기 정지 시간·WAL 보관량·부하와 고가용성은 검증하지 않았습니다.

[기존 검증 기록](verification.md): 로컬 PostgreSQL 18.6·PgBouncer 1.25.1에서 21개 테스트와 패키지 실행을 확인했습니다. 실제 클라우드 운영 검증은 아닙니다.

## 도식 다시 만들기

도식은 [caru-ini/stack-diagram](https://github.com/caru-ini/stack-diagram)의 템플릿·빌더로 만들었습니다. 도구와 아이콘 디렉터리를 준비한 뒤 실행합니다. 아이콘 출처와 도구 버전은 [architecture](architecture/)에 기록했습니다.

본문·스타일 원본은 docs/architecture-source에 있고, 노드·연결·좌표는 scripts/build_architecture.py에서 정의합니다. 출력 HTML을 직접 수정하면 다음 생성에서 덮어씁니다.

```sh
python scripts/build_architecture.py --skill-dir /path/to/skills/stack-diagram --icon-dir /path/to/icons
```

밝은·어두운 PNG는 생성한 HTML을 Chromium으로 표시해 저장합니다. 같은 HTML의 #architecture·#cutover·#rollback·#recovery를 각각 캡처합니다. HTML에는 폰트와 아이콘이 포함되므로 문서를 읽을 때 네트워크가 필요하지 않습니다.

```sh
python /path/to/skills/stack-diagram/scripts/render_png.py docs/architecture/index.html docs/architecture/architecture.png --selector '#architecture' --scale 2
```

cutover·rollback·recovery도 같은 명령에서 파일 이름과 selector를 바꿔 생성합니다. 기본 명령은 두 화면 테마를 모두 저장합니다.
