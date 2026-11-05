> PostgreSQL 상태 저장소를 사용하던 시점의 수정 기록입니다. 현재 상태 저장은 DynamoDB로 전환했습니다. 최신 동작은 [구현 설명](implementation.md)을 보십시오.

# 장애 경로 검토와 수정

검토 기준은 2e85f4a입니다. 수정은 fix/review-recovery 작업 트리에 있습니다. API 경로와 공통 요청 형식은 유지했습니다.

| 지적 | 수정과 검사 |
| --- | --- |
| 설정 재작성으로 옵션·주석 손실, include 파싱 실패 | 한 alias의 host·port·dbname 값만 교체합니다. CRLF·인증 옵션·주석·%include 보존을 단위 검사와 실제 PgBouncer RELOAD로 확인합니다. alias가 직접 정의된 파일을 등록해야 합니다. |
| GET 일시 장애가 영구 UNKNOWN으로 남음 | 관측 실패와 명령 결과 불명을 분리했습니다. 관측 실패는 진행 조건만 UNKNOWN으로 두고 재관측합니다. 실제 명령의 UNKNOWN은 RECONCILE 전까지 유지합니다. 기본 2초 캐시를 사용하고 캐시 조회에는 행 잠금을 잡지 않습니다. |
| 대상 장애가 대상 쓰기 전 복귀를 차단 | 원본이 기준 DB라는 영속 근거가 있으면 원본·프록시만으로 명시적 복귀를 수행합니다. 원본에서 자기 계획의 송신 복제를 정리하고 원본을 검증합니다. 실제 대상 컨테이너를 정지해 복귀를 검사합니다. 대상 쓰기 후 장애에는 이 경로를 사용하지 않습니다. |
| barrier 진행 LSN을 비교하지 않음 | 첫 marker 커밋 후 WAL 경계와 대상의 내구성 있는 origin 진행 LSN을 비교합니다. 뒤이은 marker로 경계를 통과하게 합니다. 이전의 NULL이 아닌 LSN을 주입해 통과하지 않는지 확인합니다. |
| 직접 롤백 중단 후 불완전한 대상과 재비교 | directRecovery 기록이 있으면 원본 schema·snapshot만 대조합니다. 경로 변경 전·후 중단을 각각 검사합니다. 송신 복제를 정리하기 전에는 원본 쓰기를 열지 않습니다. |
| 상태 경합·성공 후 관측 예외·오류 메시지 혼동 | 읽기 시작 시점의 revision과 실행 중 여부를 잠금 안에서 비교합니다. 실제 QUIESCE와 오래된 조회를 경합시켜 상태 보존을 확인합니다. 성공 후 관측 예외는 Worker를 종료하지 않습니다. GET 조회 실패와 POST 접수 불명을 다른 오류로 응답합니다. |

사용이 끝난 정방향 publication·slot은 CUTOVER에서 정리합니다. FINALIZE까지 불필요한 원본 WAL을 보관하던 경로도 줄였습니다. 역복제 slot은 롤백에 필요하므로 계속 남고, 지연에 따른 WAL 사용량은 별도 관측해야 합니다.

대상 장애 시 직접 복귀는 이 플랫폼이 writer 권한을 독점 관리하는 프로필에 한정됩니다. 대상 쓰기 허용 기록이 없고 PREPARE의 차단 근거가 남아 있어야 합니다. 다른 관리자나 앱이 권한을 변경할 수 있는 환경에는 이 근거를 적용할 수 없습니다. 원본·PgBouncer·업무 정지 근거가 불명이거나 대상 쓰기를 허용한 적이 있으면 진행하지 않습니다.

전체 digest의 실행 시간, 프록시·실행기의 고가용성, 공유 토큰과 단일 owner, 관리형 클라우드 권한 지원은 이번 수정 범위에 포함하지 않았습니다. 운영 준비가 끝났다는 판정은 아닙니다.

근거: [PgBouncer 설정과 include](https://www.pgbouncer.org/config.html), [PgBouncer 연결 문자열 파서](https://github.com/pgbouncer/pgbouncer/blob/pgbouncer_1_25_1/src/loader.c), [PostgreSQL WAL·origin 함수](https://www.postgresql.org/docs/18/functions-admin.html), [복제 진행 추적](https://www.postgresql.org/docs/18/replication-origins.html).


## 독립 검토 2차 수정

다음 9개 문제를 수정했습니다. 성공 여부는 [검증 기록](verification.md)의 최신 결과로 확인합니다.

| 문제 | 수정 |
| --- | --- |
| 원본 PAUSE 뒤 중단 시 복구 명령 없음 | 영속 명령 기록과 실제 경로를 대조합니다. 새 정지 근거로 QUIESCE 또는 RESUME SOURCE를 이어갑니다. |
| 대상 LOGIN 뒤 프록시 재개 전 중단 | RESUME TARGET만 이어갈 수 있습니다. targetWritesIntent는 유지하고 단순 ROLLBACK을 막습니다. |
| 정지 근거 갱신의 30초 초과 | 관측 시각을 한 번만 읽고 만료 시각을 계산합니다. |
| 오래된 PASS로 FINALIZE 실행 | 접수와 삭제 직전에 업무·probe·역복제·안정화를 재검사합니다. 실행 승인과 삭제 의도를 같은 트랜잭션에 기록합니다. |
| REPLICA IDENTITY NOTHING 허용 | PK와 DEFAULT replica identity를 PREPARE에서 요구합니다. |
| UNIQUE INDEX 누락 허용 | index 이름을 제외한 정의·유효성·즉시 검사 여부를 비교합니다. 부분·식 index도 포함합니다. |
| sequence 범위·형식 차이 누락 | 자료형·시작값·증가량·최솟값·최댓값·cache·순환 여부를 기준값에 저장하고 재검사합니다. |
| 새 generation에 이전 안정화 시간 재사용 | workflow·scope·topology·generation·제어 변경 번호로 30초 구간을 구분합니다. 정상 heartbeat는 시간을 유지합니다. |
| 설정 교체 후 소유자·그룹 손실 | UID·GID·mode·확장 속성을 보존합니다. 보존 불가 또는 확장 ACL은 PREPARE에서 거절합니다. |

추가 경계도 막았습니다. 반복 RECONCILE이나 실패한 RECONCILE이 원래 복구 명령의 기록을 지우지 않습니다. 최초 PAUSE만 된 상태에서 정지 기한이 지나거나 미사용 대상이 응답하지 않으면, 원본 차단·검증까지만 진행하는 복귀 전용 QUIESCE를 사용합니다. 이 시도가 다시 중단돼도 재시도할 수 있습니다. 대상 CUTOVER는 허용하지 않습니다.

FINALIZE는 승인 후 되돌릴 수 없는 정리 단계입니다. 중단되면 RECONCILE 후 같은 계획의 FINALIZE를 다시 요청합니다. 이미 삭제한 자기 계획의 복제 객체는 건너뛰고 나머지를 정리합니다. 이 구간에는 같은 업무 범위의 새 HELD/RUNNING 제어 변경을 거절합니다. 앱을 실제로 멈추는 동작은 여전히 오케스트레이터의 책임입니다.
