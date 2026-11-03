# db-migration-adaptor

Foxtrot의 PostgreSQL 마이그레이션 어댑터입니다. 공통 API의 명령을 PostgreSQL 논리 복제와 PgBouncer 제어 작업으로 연결합니다.

현재는 설계 정리 단계입니다. 실행 API와 마이그레이션 Worker는 아직 구현하지 않았습니다.

## 구현 범위

- 등록된 원본·대상 DB와 PgBouncer의 연결·권한·복제 설정을 검사합니다.
- PostgreSQL 기본 논리 복제로 초기 데이터와 변경분을 복사합니다.
- 업무 쓰기를 정지한 뒤 마지막 변경 적용, 데이터, 시퀀스를 검사합니다.
- PgBouncer의 대상 DB를 변경하고 별도 명령으로 업무를 재개합니다.
- 실행 의도, 실제 변경, 검증 근거를 별도 관리 DB에 저장합니다.
- 결과가 불명이면 UNKNOWN으로 중지하고 실제 상태를 대조합니다.

첫 구현은 PostgreSQL 기본 논리 복제와 PgBouncer를 사용합니다. 클라우드별 권한과 네트워크 조건은 실제 시험을 통과한 조합만 지원으로 표시합니다.

## 전체 흐름에서의 역할

```text
오케스트레이터 → 공통 API → 실행 기록 → DB Worker
                                      ├ 원본·대상 PostgreSQL 제어
                                      └ PgBouncer 설정·관리 명령 실행

앱·업무 Worker → 고정 PgBouncer 주소 → 현재 활성 DB
```

오케스트레이터는 리소스의 순서와 업무 제어를 맡습니다. 이 어댑터는 DB 한 리소스의 복사·전환·검증을 맡습니다. 앱의 DB 접속 설정 주입은 COMPUTE 어댑터와 배포 계층이 담당합니다.

sourceRef와 targetRef는 등록된 DB를 가리킵니다. bindingRef는 PgBouncer alias, 관리 접속, Secret, 실행기와 설정 참조를 가리킵니다. 요청에는 비밀번호나 임의 실행 명령을 넣지 않습니다.

## 구현 예정 API

| Method | Path | 역할 |
| --- | --- | --- |
| GET | /v1/capabilities | 지원 프로필 조회 |
| POST | /v1/migrations | 이전 계획 등록 |
| GET | /v1/migrations/{migrationId} | 현재 상태와 진행 조건 조회 |
| POST | /v1/migrations/{migrationId}/actions | 실행 명령 접수 |
| GET | /v1/operations/{operationId} | 명령 실행 결과 조회 |

명령은 PREPARE, START_SYNC, QUIESCE, CUTOVER, RESUME, ROLLBACK, CANCEL, RECONCILE, FINALIZE를 사용합니다.

CUTOVER는 접속 경로 변경까지 수행하고 업무 정지는 유지합니다. RESUME은 지정한 위치의 업무를 재개합니다. 대상에서 새 쓰기가 가능해진 뒤에는 역복제와 최신성 검증 없이 원본으로 되돌리지 않습니다.

## 설계 문서

- [마이그레이션 공통 Design Doc 초안](https://app.notion.com/p/40d8bee9ada4815e9e94ee454be82d4a)
- [PostgreSQL 이전·전환·롤백 핵심 설계](https://app.notion.com/p/40c8bee9ada481b0b268cee206e3e51f)

다음 리소스 진행 판정, 업무 정지 근거, 새 transitionId 허용 조건, 앱용 연결 참조의 전달 형식은 공통 설계에서 확정해야 합니다.
