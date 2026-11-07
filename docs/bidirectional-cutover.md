# 양방향 이전과 정지 구간 단축 (2026-11-08)

`postgres-managed-logical-pgbouncer-v2`를 RDS ↔ Cloud SQL 어느 방향으로도 반복 실행할 수 있게 하고,
CUTOVER 한 명령 안의 쓰기 정지 구간을 줄입니다. 기록은 작업 순서대로 갱신합니다.

## 출발점

- 2026-11-08 02:46 KST 클라우드 E2E(wf-55a54889)에서 RDS → Cloud SQL 전환의 앱 정지 구간은 약 12초였습니다. 단계별 시간은 기록되지 않았습니다.
- 코드는 원본 `AWS_RDS`, 대상 `GCP_CLOUDSQL`만 받습니다(`service.create`, `ManagedLogicalPostgres.preflight`). 대상 메타데이터 검사는 Cloud SQL Admin API 전용입니다.
- 되돌아가는 이전은 대상(이전 원본)의 업무 table이 비어 있지 않아 `TARGET_NOT_EMPTY`로 막힙니다. 이전 이전의 복제 객체가 남아 있으면 `_check_isolation`도 막습니다.
- 정지 구간 안에서 실행되는 것: 관측(`observe`) 5회, `validate` 3회, 양쪽 schema 전체 서명 2회(table마다 6개 질의), 실행 의도 기록마다 DynamoDB 트랜잭션 2회, 50ms 폴링.

## 단계

| # | 내용 | 상태 |
|---|---|---|
| 1 | 방향 제한 제거: 원본·대상 모두 관리형(RDS, Cloud SQL)이면 허용. 인스턴스 메타데이터는 제공자별 검사(Cloud SQL Admin API, RDS DescribeDBInstances). 원본 logical WAL 필수 | 예정 |
| 2 | 되돌아가는 이전: 완료(COMPLETED)된 이전 이전의 원본이던 대상을 PREPARE에서 초기화. 대상 writer NOLOGIN·세션 0·경로 SOURCE를 확인한 뒤 등록 table TRUNCATE와 barrier 삭제. 완료된 이전의 복제 객체 잔여물도 정리 | 예정 |
| 3 | 정지 구간 단계별 시간 기록(`internal.cutoverTimeline`, `writePauseMs`)과 관측 API 노출 | 예정 |
| 4 | 정지 구간 단축: 실행 의도 기록과 정지 근거 검사를 한 트랜잭션으로, 정지 중 schema 검사를 한 질의 digest로, 같은 명령 안의 관측·validate 재사용, 폴링 10ms | 예정 |
| 5 | 로컬 랩(PostgreSQL 17) 양방향 통합 검사와 전체 검사 통과 | 예정 |
| 6 | IaC: RDS가 Cloud SQL 인증서 이름을 풀 수 있도록 Route53 private zone을 앱 VPC에도 연결 | 예정 |

## 결과

아래 "검증 기록"에 각 단계의 검사 결과와 측정값을 적습니다.
