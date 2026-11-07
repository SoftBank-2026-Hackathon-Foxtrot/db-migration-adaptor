# Redis와 같은 DB 명령 동작

새 프로필은 `postgres-managed-logical-pgbouncer-v2`입니다. 기존 v1 계획은 동작을 유지합니다. 새 동작을 쓰려면 v2로 계획을 등록합니다. DMS와 Promote는 사용하지 않습니다.

| 명령 | DB 에이전트가 하는 일 | 완료 상태 |
|---|---|---|
| `QUIESCE` | 원본 운영·schema·초기 복제 완료 확인. 쓰기는 계속 허용 | `QUIESCED` |
| `CUTOVER` | PAUSE → 원본 writer 차단 → 최종 barrier·적용 LSN 확인 → subscription 정지 → sequence 맞춤 → 원본 slot 정리 → 경로 변경 → 대상 writer·프록시 재개 | `SWITCHED`, 대상 쓰기 허용 |
| `RESUME` | 이미 재개한 경로·권한·복제 완료 근거 확인. DB와 프록시 변경 없음 | `ACTIVE` 또는 `RESTORED` |
| `ROLLBACK` | 대상 쓰기 전 원본 경로 복귀와 DB 쓰기 재개 | `ROLLED_BACK_HELD`, 원본 쓰기 허용 |

`QUIESCED`와 `ROLLED_BACK_HELD`는 공통 API의 상태 이름입니다. v2에서는 실제 쓰기 정지를 뜻하지 않습니다. 쓰기 가능 여부는 `observed.businessWriteAccess`와 실제 프록시 상태로 확인합니다. Redis도 같은 상태 이름과 명령 의미를 사용합니다.

## 앱 업무 정지

오케스트레이터는 `QUIESCE`를 앱 업무가 실행 중인 상태에서 호출합니다. 준비 확인을 통과하면 앱·업무 Worker의 HELD와 진행 요청 0건을 확인합니다. 그때 발급한 실제 `quiesceRef`로 `CUTOVER`를 보냅니다. `CUTOVER` 안에서 DB 쓰기를 다시 열고, `RESUME`이 결과를 확인하면 오케스트레이터가 앱 업무를 RUN으로 바꿉니다.

DB는 Redis와 달리 `CUTOVER`·`ROLLBACK`에서 실제 업무 정지 근거를 계속 검사합니다. `QUIESCE`·`RESUME`의 참조는 시도 식별용이며 HELD 기록으로 쓰지 않습니다. 검사 전용 참조를 `CUTOVER`에 보내면 거절합니다. 두 명령에서도 `transitionId`, 인증, revision과 멱등 키 검사는 유지합니다.

## 중간 실패

| 실제 상태 | RECONCILE 뒤의 처리 |
|---|---|
| 준비 확인 실패, 원본 쓰기 정상 | 동기화 상태를 다시 읽고 준비 확인 재시도 |
| 원본에서 PAUSE·차단·최종 복제 중 중단 | 쓰기 정지를 유지. 새 정지 근거로 전환 재시도 또는 원본 복귀 |
| 대상 경로 변경 뒤 쓰기 재개 전 중단 | 최종 복제 근거와 writer·연결 상태를 대조. `CUTOVER`로 재개 작업 완료 |
| 대상 쓰기 재개 뒤 응답 유실 | 대상 운영을 유지. `RESUME`으로 결과 확인. 전환을 다시 실행하지 않음 |
| 원본 복귀·재개 뒤 응답 유실 | 원본 운영을 유지. `RESUME expectedLocation=SOURCE`로 확인 |
| 실제 경로·권한·복제 근거 불명 | UNKNOWN 유지. 다음 외부 변경 차단 |

쓰기 정지 제한을 넘기면 새 전환을 시작하지 않습니다. 대상 쓰기 허용 전에는 원본 복귀가 가능합니다. 대상 재개 의도와 경로가 이미 기록됐고 프록시가 멈춘 경우에는 RECONCILE로 남은 재개 작업을 확인한 뒤 그 작업만 완료할 수 있습니다. 이 복구에도 최신 HELD 근거가 필요합니다.

대상 쓰기 허용 의도를 기록한 뒤에는 원본 복귀를 거절합니다. 실제 쓰기 0건으로 보이더라도 역복제 없이 되돌리지 않습니다. 역방향 CDC와 유실 감수 롤백은 지원하지 않습니다.

## 검증 범위

실제 PostgreSQL 17·PgBouncer·DynamoDB Local에서 정상 전환, 준비 확인 중 원본 쓰기, CUTOVER 직후 대상 쓰기, 검사 전용 참조 거부, 응답 유실, 원본 복귀, 부분 재개와 정지 제한 초과를 검사합니다. Cloud SQL 메타데이터 응답은 모의입니다. 실제 RDS·Cloud SQL에서 v2의 전환 시간을 다시 측정한 결과는 아닙니다.

DB 단위 70개, PostgreSQL 17 관련 통합 18개, 오케스트레이터 186개가 통과했습니다. 외부 Redis·오브젝트·트래픽 서버가 필요한 오케스트레이터 검사 5개는 실행하지 않았습니다. 기존 v1 통합 검사도 포함하며, DB의 전체 테스트 모음을 모두 실행한 결과는 아닙니다.

기존 [v1의 8.58초 실측](performance/2026-11-06-native-e2e.json)은 별도 기록입니다. 현재 실행 중인 v1 클라우드 시연은 자동 변경하지 않습니다.
