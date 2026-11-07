# DB OTEL 관측

`src/db_operator/api.py`의 독립 5초 observer가 등록된 migration을 읽어 snapshot과 수치를 발행합니다. `worker.py`는 실제 명령을 span으로 감싸고 접수 시 저장한 W3C trace context를 이어갑니다. 완료/실패 뒤 snapshot도 발행합니다. CP의 별도 관측 polling은 없습니다.

`details.database`: PgBouncer pause, 양쪽 writer/prepared 수, targetReachable, accessEvidence, 전환에 연결된 검증 근거. 관측 실패는 `observationAvailable=false`, 없는 수치는 null입니다. durable barrier는 전체 행 비교가 아닙니다. 대상 쓰기 후 복귀 가능 여부는 사용하는 프로필의 실제 근거를 따릅니다.

검사: `uv run ruff check .`, `uv run ruff format --check .`, `uv run pytest`. DB·DynamoDB 통합은 기존 opt-in lab이 필요합니다. 기존 lab의 managed/atomic TLS 실패는 이 변경으로 해결하지 않습니다.

## OTEL 실행 설정

공식 OpenTelemetry SDK가 OTLP HTTP/protobuf로 로그·메트릭·트레이스를 내보냅니다. 아래 값을 **프로세스 환경**에 주입합니다. endpoint가 없거나 `OTEL_SDK_DISABLED=true`이면 전송하지 않습니다. `.env` 파일을 만드는 것만으로 Python 프로세스 환경이 바뀌지는 않습니다.

```dotenv
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.internal:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<module-ingest-token>
```

Collector는 IaC의 `observability/compose.yaml`로 준비합니다. Collector → CP에는 별도 `OBSERVATION_TOKEN`을 씁니다. 모듈에 CP 수신 토큰을 주지 않습니다. 운영 주소는 사설망/VPN에서 접근하도록 구성하고, TLS를 사용하는 경우 SDK의 `OTEL_EXPORTER_OTLP_CERTIFICATE`와 HTTPS endpoint를 설정합니다. 앱 환경 값·DSN·Terraform output·원시 키·오류 원문은 전송하지 않습니다.

진행 상태는 OTEL 구조화 로그 → Collector → CP의 `OBS_LATEST`/24시간 이력, 수치는 Prometheus, 작업 span은 Tempo에 저장합니다. 플랫폼은 CP API만 조회합니다. 제어 명령·승인·멱등성·판정 근거는 기존 저장소가 원본이며 OTEL 전송 성공을 전환 조건으로 사용하지 않습니다. 관측이 끊겨도 이전 값을 0이나 성공으로 바꾸지 않습니다.

Python `telemetry.py`/`telemetry_contract.py`는 독립 이미지에 포함한 공통 계약 v1 사본입니다. 계약을 바꾸면 CP와 모든 Python 발행자의 사본 및 SDK 계약 테스트를 함께 갱신합니다. metric에는 operation/transition ID·키·URL을 label로 넣지 않으며, 값 캐시는 최대 2048 series/20초입니다. workflow ID는 마이그레이션별 화면 조회를 위한 제한된 차원입니다.
