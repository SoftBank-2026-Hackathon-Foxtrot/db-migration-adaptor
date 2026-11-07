# 마이그레이션 관측

`GET /v1/migrations/{id}`의 `details.database`는 PgBouncer pause, 양쪽 writer/prepared 수, targetReachable, accessEvidence와 검증 근거를 제공합니다. DSN·SQL·행 데이터는 포함하지 않습니다. 관측 실패는 observationAvailable=false, 없는 수치는 null입니다.

검증은 전환 ID에 연결된 과거 작업 근거입니다. `NATIVE_DURABLE_BARRIER`는 최종 변경의 durable 적용 근거이며 전체 행 비교와 다릅니다. 기본 관리형 프로필에 역복제를 추가하지 않습니다. 작업 API에는 requestedAt/startedAt/finishedAt/phase가 추가됩니다. 컨트롤 플레인이 5초마다 상태를 읽습니다.

검사: `uv run ruff check .`, `uv run pytest`. 실제 DB·DynamoDB lab 통합 검사는 기존 opt-in 환경이 필요합니다. 테스트가 참조하는 httpx도 개발 의존성에 명시했습니다.

2026-11-07 검증에서 기본 lab의 PostgreSQL은 SSL을 지원하지 않아 managed/atomic 프로필의 PREPARE가 실패했습니다. 이 프로필의 fixture는 TLS 연결을 요구합니다. 변경 전 main 소스로도 같은 실패를 재현했습니다. 해당 통합 검사를 완료하려면 TLS가 구성된 PostgreSQL lab과 신뢰할 인증서를 준비해야 합니다. DMS 설정 변경 후 성공 확정을 거부하는 검사는 로컬 DynamoDB에서 별도로 확인했습니다.
