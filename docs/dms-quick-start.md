# DMS Quick-start 적용 검토

2026-11-06 기준입니다. 공식 문서, 실제 GCP API 응답과 설치한 SDK를 확인했습니다. Quick-start 작업을 생성하거나 전환 성능을 측정한 결과는 아닙니다.

후속 결정: 사용자가 직접 논리 복제를 선택해 [관리형 논리 복제 프로필](managed-logical.md)을 구현하고 실제 전환을 측정했습니다. 아래 내용은 적용하지 않은 Quick-start의 검토 기록입니다.

## 판단

Quick-start는 적용 후보입니다. 현재 공통 API 명령은 유지할 수 있지만, DMS 연결과 완료 확인 코드는 바꿔야 합니다. 기존 작업에서 `Promote`만 빼는 방식으로 적용하지 않습니다.

Quick-start도 원본 쓰기를 멈추고 최종 변경을 반영한 뒤 `Promote`를 호출합니다. 표준 방식과 달리 이 호출은 대상 인스턴스 변경 없이 복제를 종료합니다. 따라서 기존 승격 40.07초를 줄일 가능성은 있지만, 단축 시간은 아직 알 수 없습니다. [Quick-start 완료 절차](https://docs.cloud.google.com/database-migration/docs/postgres/quick-start-migrations-guide#finalize_the_migration)

## 현재 환경에서 확인한 것

| 항목 | 확인 결과 | 필요한 변경 |
|---|---|---|
| 운영 대상 Cloud SQL | 공인 IP만 사용. 사설 IP와 PSC 설정 없음 | DMS용 사설 연결 준비 |
| 중지한 벤치마크 대상 Cloud SQL | 공인 IP만 사용. `activationPolicy=NEVER` | 측정 대상을 정한 뒤 연결 준비 |
| 서울 리전 DMS private connection | 조회 결과 0개 | Quick-start용 연결 생성 |
| 기존 시연 DMS 작업 | 표준 방식, `COMPLETED` | 새 Quick-start 작업 생성 |
| GCP REST v1 | `postgresHomogeneousConfig.isNativeLogical` 지원 | REST로 원본 JSON 조회·실행·결과 확인 |
| Python `google-cloud-dms` 1.17.0 | 설치 버전과 PyPI 최신 버전이 같음. native 설정·연결 프로필 role·새 연결 설정 필드 없음 | SDK 버전 변경만으로 해결하지 않음 |
| 설치한 gcloud 587.0.0 | `--use-postgres-native` 지원 | 수동 사전 검증에 사용 가능 |

API 조회는 리소스를 변경하지 않았습니다. 기존 DB와 시연은 이 검토로 전환하거나 재시작하지 않았습니다.

## 연결 방식

```text
앱 → 기존 PgBouncer → Cloud SQL 공인 접속 주소

AWS RDS 공인 주소 → DMS 중계 계층 → Cloud SQL 사설 연결
```

Quick-start는 원본의 공인 IP 접속을 지원합니다. 대상에는 사설 IP가 필요하며 공인 IP를 함께 사용할 수 있습니다. 앱 접속에 VPN이나 Tailscale을 추가할 필요는 없습니다. DMS가 대상에 접속하는 경로를 별도로 준비합니다. [연결 조건](https://docs.cloud.google.com/database-migration/docs/about-quick-start-migrations#overview)

원본 접속에는 public IP용 DMS private connection을 사용하고, 그 연결에서 확인한 송신 IP만 RDS 보안 그룹에 허용합니다. 기존 Cloud SQL의 OUTGOING IP를 그대로 쓸 수 있다고 가정하지 않습니다. [공인 IP 연결 준비](https://docs.cloud.google.com/database-migration/docs/postgres/quick-start-migrations-guide)

## 코드 변경 순서

1. **Quick-start용 DMS 연결을 따로 등록합니다.** `isNativeLogical=true`, 대상 DB 선택, 연결 프로필 role과 네트워크 설정을 명시합니다. 표준 작업을 자동 변경하지 않습니다. 대상에 같은 이름의 운영 DB가 있으면 덮어쓰지 않습니다.
2. **DMS REST 연동을 추가합니다.** SDK가 모르는 필드까지 원본 JSON으로 검사하고 fingerprint에 포함합니다. 작업·원본·대상·owner를 대조하고, 명령을 보내기 전에 의도를 저장합니다. 응답이 불명확하면 재전송하지 않고 기존 실행 ID를 확인합니다.
3. **최종 복제 완료 근거를 검토합니다.** 여러 subscription을 사용하면 한 barrier의 도착만으로 모든 스트림의 적용 완료를 보장할 수 없습니다. 스트림별 완료 위치를 확인하거나, 단일 스트림임을 확인한 구성으로 범위를 제한합니다. 동기화 상태를 확인하지 못하면 전환하지 않습니다.
4. **기존 공통 API에 연결합니다.** PREPARE → START_SYNC → QUIESCE → CUTOVER → RESUME은 유지합니다. CUTOVER에서 Quick-start의 Promote 완료를 확인한 뒤 sequence·권한을 준비하고 PgBouncer 경로를 바꿉니다. 대상 쓰기 후 역복제 없는 원본 복귀는 계속 거절합니다.
5. **별도 대상에서 실제 측정합니다.** 원본 운영 쓰기와 기존 시연을 분리합니다. CDC까지 준비한 뒤 정지·복제 완료 확인·Promote·경로 변경·쓰기 재개의 시간을 각각 기록합니다. 검증 후 테스트 대상과 복제 작업의 종료 상태도 확인합니다.

Quick-start 작업은 REST API의 `postgresHomogeneousConfig`로 구분합니다. 기존 Python SDK의 작업 응답만 보고 표준 방식인지 판단하면 새 필드를 놓칠 수 있습니다. [REST 필드](https://docs.cloud.google.com/database-migration/docs/reference/rest/v1/projects.locations.migrationJobs#PostgresHomogeneousConfig)

## 채택 조건

- 동시 쓰기 중 초기 복사와 CDC가 따라오는지 확인합니다.
- 전환 직전까지 커밋한 변경이 대상에 있고, sequence가 새 쓰기와 충돌하지 않아야 합니다.
- 대상에서 새 쓰기가 성공하고 원본 writer는 차단돼 있어야 합니다.
- Promote 응답 유실 시 재호출 없이 기존 실행 결과로 복구해야 합니다.
- 쓰기 정지부터 재개까지의 시간이 기존 67.18초보다 짧은지 같은 조건에서 측정합니다. 이 수치는 원본 추가 쓰기 0건 조건의 기존 결과입니다. 쓰기 부하가 있는 결과는 따로 비교합니다.

Quick-start는 현재 Preview입니다. DMS의 원본 TLS 옵션은 `none`과 `required`만 지원합니다. 앱·Operator의 `verify-full` 접속과 별개이므로, 이 차이를 적용 전에 검토합니다. [지원 범위](https://docs.cloud.google.com/database-migration/docs/about-quick-start-migrations#limitations)

현재 결론은 **적용 가능성을 확인했으며, 네트워크와 REST 연동을 먼저 준비해야 한다**입니다. 성능 개선과 운영 채택은 아직 검증하지 않았습니다.
