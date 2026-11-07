# 검증 기록

## 2026-11-08: 실제 RDS ↔ Cloud SQL 양방향 전환과 정지 구간 실측

[양방향 이전과 정지 구간 단축](bidirectional-cutover.md)을 적용한 이미지(877c392)로 chiikawa 앱의 실제 DB를 Cloud SQL → RDS(wf-32f75936, 05:23 KST), 다시 RDS → Cloud SQL(wf-93f2a371, 05:53 KST)로 옮겼습니다. 두 번째는 Redis·오브젝트와 함께 한 워크플로에서 옮겼습니다. 오케스트레이터 쪽 기록은 control-plane의 [E2E 실행 기록](../../control-plane/docs/e2e/2026-11-07-08.md)입니다.

| | Cloud SQL → RDS | RDS → Cloud SQL |
|---|---:|---:|
| 쓰기 정지 `writePauseMs`(PAUSE 의도 → RESUME 의도) | **1,409** | **1,446** |
| CUTOVER 명령 전체 | 2,109 ms | 2,286 ms |
| 사전 검사(PAUSE 전) | 233 | 272 |
| PAUSE | 104 | 122 |
| writer 차단·연결 확인 | 176 | 170 |
| 최종 barrier 커밋·적용 확인 | 290 | 277 |
| subscription 정지 | 78 | 82 |
| 검증·sequence | 102 | 105 |
| publication·slot 정리, 설정 교체, RELOAD | 407 | 435 |
| 재개 검사 | 65 | 80 |
| writer 허용 | 74 | 96 |
| RESUME → 앱 연결 확인 | 179 | 275 |

같은 프로필의 전날 실행(wf-55a54889, 변경 전 이미지)은 CUTOVER 명령 2.54초였고 구간별 기록이 없습니다. 되돌아가는 실행에서는 PREPARE가 이전 원본의 업무 table과 barrier를 비운 뒤 통과했습니다.

남은 문제: Cloud SQL → RDS의 START_SYNC가 CREATE SUBSCRIPTION 응답 미확인으로 8번 UNKNOWN이었고 RECONCILE 뒤 9번째에 성공했습니다. 원인은 아직 좁히지 못했습니다. 첫 되돌림 시도(wf-b0ad384a)는 같은 구간에서 `REPLICATION_CONFLICT`로 취소했습니다.

## 2026-11-06: Redis와 명령 의미를 맞춘 v2

`postgres-managed-logical-pgbouncer-v2`에서는 QUIESCE가 준비만 확인하고, CUTOVER가 DB 정지·최종 복제·경로 변경·쓰기 재개를 완료합니다. RESUME은 읽기 검사만 합니다. ROLLBACK은 대상 쓰기 전 원본 복귀와 DB 재개를 포함합니다. 기존 v1 계획과 실행 중인 클라우드 시연은 변경하지 않았습니다.

DB 단위 70개, PostgreSQL 17·PgBouncer·DynamoDB Local 관련 통합 18개, 오케스트레이터 186개가 통과했습니다. 외부 서버가 필요한 오케스트레이터 검사 5개는 미실행입니다. 새 동작의 정상 전환, 검사 전용 참조 거부, 응답 유실, 부분 재개, 정지 제한 초과, 미사용 대상 장애, 취소·준비 확인 복구와 과거 barrier 재사용 차단을 포함합니다. Ruff, mypy와 Docker 이미지 빌드도 확인합니다.

아래 8.58초·2.82초는 기존 v1의 클라우드 실측입니다. v2의 클라우드 전환 시간은 아직 재측정하지 않았습니다. 상세 조건은 [v2 명령 동작](atomic-cutover.md)에 있습니다.

## 2026-11-06: 직접 논리 복제의 처음 등록부터 전체 실행

별도 RDS·Cloud SQL 테스트 DB를 준비하고 원본 주문 50,000건·상품 128개, 대상 주문 0건으로 시작했습니다. 미등록 앱에서 `MIGRATE_ALL` 한 번으로 REGISTER → CREATE → PREPARE → START_SYNC → QUIESCE → CUTOVER → RESUME을 완료했습니다. 복사 중 원본 쓰기를 계속했고 전환 시 원본 주문은 50,022건이었습니다.

앱 등록 시작부터 쓰기 재개까지 **8.58초**, 대상 첫 주문 커밋까지 **8.78초**, 쓰기 정지부터 재개까지 **2.82초**였습니다. DB·schema 준비와 컨테이너 시작은 제외한 1회 실측입니다. 이전 7.50초는 READY에서 시작한 별도 정지 구간 측정이므로 전체 시간으로 사용하지 않습니다.

ACTIVE / OK, 대상 새 주문과 쓰기 오류 0건, 원본 writer NOLOGIN·대상 writer LOGIN, 원본 복제 slot·publication 정리와 대상 subscription 정지·작업 종료를 확인했습니다. 원본·대상 모두 주문 수와 재고의 합은 1,000,000이었습니다. 원래 RDS 업무 원장과 5190·5193의 기존 앱은 정상 운영을 유지했습니다. 측정용 컨테이너를 중지하고 이번에 추가한 접속 허용 규칙을 제거했습니다. 추가 클라우드 인스턴스는 만들지 않았습니다.

근거는 [전체 실행 성능 JSON](performance/2026-11-06-native-e2e.json)과 Git에서 제외한 `runtime/native-e2e-20261106`의 `timing.json`, `execution-result.json`, `progress.jsonl`, `final-proof.json`입니다.

## 2026-11-06: 직접 논리 복제로 실제 Cloud SQL 전환

`postgres-managed-logical-pgbouncer-v1`을 추가했습니다. 기존 RDS와 재가동한 벤치마크 Cloud SQL 안의 별도 DB에서 기본 publication/subscription으로 초기 복사·CDC·전환을 실행했습니다. DMS 작업과 Promote는 사용하지 않았습니다.

초기 주문 50,000건·상품 128개, 원본 주문 생성 중인 조건입니다. 전환 시 원본은 50,849건이며 쓰기 정지부터 재개까지 **7.50초**, 대상 첫 쓰기 확인까지 **7.91초**였습니다. 대상 새 쓰기·오류 0건, 원본 writer 차단, subscription 정지·slot 정리, 혼합 트랜잭션 결과와 재고 불변식을 확인했습니다. 원래 RDS 업무 데이터와 기존 5190 시연은 유지했습니다.

단위 69개, 실제 PostgreSQL 17·TLS 로컬 랩의 관리형 프로필 통합 5개, 기존 자체 복제·binding·DMS 통합 46개로 관련 검사 **120개**가 통과했습니다. Ruff·포맷·diff 검사, Docker 빌드, 실행 중 이미지의 구현 파일 일치와 실제 독립 Cloud SQL 메타데이터 검사도 통과했습니다. 전체 테스트 모음을 실행한 결과는 아닙니다.

이번 조건과 DMS의 기존 주문 549,370건·추가 쓰기 0건 조건은 다릅니다. 직접적인 성능 배수나 전환 시간 상한을 주장하지 않습니다. 대상 쓰기 후 역복제는 지원하지 않습니다. 앱·관리 연결은 verify-full이며, 실제 subscription 연결은 인증서·호스트 이름을 검증하지 않는 TLS require였습니다. 구현과 범위는 [관리형 논리 복제](managed-logical.md), 수치는 [성능 JSON](performance/2026-11-06-native.json)에 있습니다.

## 2026-11-06: 개선 후 실제 Cloud SQL 전환 재측정

별도 `foxtrot-fast-bench-20261106`에서 같은 RDS 주문 549,370건을 소형 Cloud SQL(db-g1-small, SSD 10GB)로 이전했습니다. 원본 추가 쓰기는 0건입니다. 등록·사전 검사·복사부터 앱 재개까지 **323.17초(5분 23초)**, 쓰기 정지부터 앱 재개까지 **67.18초**였습니다. 대상의 첫 주문 커밋까지는 67.57초였습니다.

| 쓰기 정지 구간 | 이전 실측 | 개선 후 실측 |
| --- | ---: | ---: |
| 정지 시작 → Promote 요청 | 24.44초 | 12.78초 |
| DMS Promote | 46.50초 | 40.07초 |
| 승격 완료 확인·경로 변경·앱 재개 | 37.13초 | 14.32초 |
| 합계 | 108.07초 | 67.18초 |

제어 구간은 61.58초에서 27.10초로 줄었고, DMS 자체 승격 시간도 6.42초 짧았습니다. 두 변화의 합으로 이번 쓰기 정지는 40.90초(37.8%) 줄었습니다. 각 한 번의 클라우드 실측이므로 서비스 전체 부하에서의 보장값은 아닙니다.

Promote RPC와 CUTOVER 실행은 각각 한 번이었고, 승격 중 UNKNOWN·RECONCILE 없이 같은 작업에서 끝났습니다. API ACTIVE / OK, DMS COMPLETED, 같은 PgBouncer 주소를 통한 대상 새 주문 쓰기·오류 0건, 원본 주문 수·최대 ID·재고·writer NOLOGIN 보존을 확인했습니다. 운영 앱은 기존 Cloud SQL에서 유지했습니다. 측정 후 새 테스트 DB와 컨테이너를 중지하고 테스트용 접속 허용 규칙을 제거했습니다. 데이터는 보존했습니다.

## 2026-11-06: 승격 대기와 반복 조회 개선

Promote는 같은 실행에서 최대 180초 기다립니다. 남은 쓰기 정지 기한을 넘기지 않으며, 잠금·정지 확인 갱신 검사는 계속합니다. 진행 중에는 저장한 클라우드 실행 ID를 조회하고, 완료 후 DMS 설정을 다시 대조합니다. 실행 ID가 없는 응답 유실은 자동 재요청하지 않습니다.

한 실행 안에서 대상 업무 DB 접속에 성공한 뒤에는 존재 여부를 다시 조회하지 않습니다. 실제 DB·DMS 상태 확인은 계속하며, 접속 실패를 maintenance DB로 우회하지 않습니다. DB 상태 조회 SQL은 5회에서 1회로 합쳤습니다. 대상 writer 권한 설정도 한 연결에서 실행하되 각 변경 전 실행 의도·잠금·정지 근거를 확인합니다.

실제 운영 중인 Cloud SQL에서 SELECT만 실행해 전후를 비교했습니다. 한 실행 범위의 대상 상태 조회 3회를 3회씩 반복한 중앙값은 **3.04초 → 1.21초**였습니다. 이 범위의 클라우드 조회는 12회에서 4회로 줄었습니다. 전체 전환 시간의 재측정 결과는 아닙니다.

단위 검사 66개, 실제 PostgreSQL·PgBouncer·DynamoDB와 모의 GCP SDK를 연결한 DMS 통합 검사 27개, 자체 복제 전환·복귀 검사 7개, 앱 등록·해제 검사 12개가 통과했습니다. 총 112개이며 중복 실행은 세지 않았습니다. 46초 승격의 동일 실행 완료, 남은 정지 기한과 최대 대기 시간 준수, 완료 직전 설정 변경 차단, 권한 설정 도중 중단 후 대상 쓰기 차단·복구를 포함합니다.

기존 랩 설정에 `auth_type = scram-sha-256`처럼 공백이 있으면 검사 fixture가 HBA 전환을 생략하던 문제도 수정했습니다. 전환 전후 코드에서 같은 실패를 재현했습니다. fixture가 실제 PgBouncer의 HBA 활성화를 확인하도록 고친 뒤 앱 간 접속 차단과 DMS 검사를 다시 통과했습니다.

Docker 이미지 `foxtrot/db-operator:fast-cutover`를 로컬 Cloud SQL 대시보드의 API·Worker에 적용했습니다. 같은 migration ID·대상 DB 경로, 원본 writer NOLOGIN·주문 수 보존, 대상 새 주문 쓰기와 오류 0건을 확인했습니다. PgBouncer와 DB는 재시작하지 않았습니다. 시험 랩 컨테이너는 정지하고 볼륨은 보존했습니다. 이후 실제 클라우드 재측정 결과는 위 기록에 있습니다.

## 2026-11-05: 전수 비교 제거 후 실제 Cloud SQL 전환

별도 `foxtrot-fast-bench-20261105`에서 RDS 주문 549,370건을 복사했습니다. 원본 추가 쓰기는 0건이며, 운영 앱은 기존 Cloud SQL에 유지했습니다. 등록·PREPARE부터 앱 재개까지 **372.09초(6분 12초)**, 쓰기 정지는 **108.07초**였습니다. 전체 372.09초에는 최초 등록과 사전 검사가 포함됩니다.

쓰기 정지부터 Promote 요청까지 24.44초, Promote 자체 46.50초, 완료 확인·경로 변경·재개까지 37.13초였습니다. 대상의 새 주문 쓰기, 원본 주문 수·최대 ID·재고 보존, 원본 writer NOLOGIN을 확인했습니다. 측정 후 새 테스트 DB와 컨테이너만 중지했습니다. 이 기록은 승격 대기·조회 개선을 적용하기 전 측정입니다.

## 2026-11-05: DMS 전수 데이터 비교 제거

사용자 요청에 따라 DMS의 QUIESCE·CUTOVER·RESUME·ROLLBACK·RECONCILE에서 전체 table hash 비교와 반복 schema 대조를 제거했습니다. 복제 완료는 현재 migration·transition의 `completionEvidence`와 barrier로 확인합니다. 쓰기 차단·정지 확인 갱신·DMS 승격 완료·sequence 설정·권한·실제 앱 접속 경로 확인은 유지합니다. PREPARE와 START_SYNC 전 지원 조건 검사도 유지합니다.

단위 검사 61개가 통과했습니다. DMS 통합 시험에서는 `Postgres.snapshot`·`Postgres.equal`이 호출되면 실패하도록 설정했습니다. 정상 전환·복귀·재전환·RECONCILE, 누락된 barrier 차단, 기존 전수 비교 기록의 호환을 확인했습니다. 기존 자체 복제 프로필의 전환·역복제·복귀 회귀 2개도 함께 확인했습니다. 승격 진행 중의 완료 근거 처리를 보완한 뒤 관련 4개 시험을 다시 실행해 통과했습니다.

Docker 이미지에 변경을 반영했습니다. 이미 전환한 Cloud SQL 앱의 운영 상태는 유지했습니다. 아래 7분 9초·쓰기 정지 3분 22초는 전수 비교 제거 전 기록입니다. 전수 비교 제거 후 실측은 위 별도 기록에 있습니다. 이 구성은 원본·대상 행 내용의 일치를 전수 검사하지 않습니다.

증거는 Git에서 제외한 `runtime/cloud-start-20261105`의 `no-data-scan-tests.xml`, `no-data-scan-recheck.xml`, `no-data-scan-build.log`에 있습니다.

## 2026-11-05: 실제 Cloud SQL 전환·앱 재개 완료

실제 RDS → GCP DMS → Cloud SQL에서 START_SYNC → QUIESCE → CUTOVER → RESUME TARGET을 실행했습니다. 대상은 `foxtrot-demo-start-20261105`입니다. 앱 등록·사전 검사가 끝난 상태에서 시작했으며, 복사 시작부터 앱 재개까지 **429.35초(7분 9초)**, 앱 쓰기 정지는 **202.32초(3분 22초)**였습니다. 최초 등록·사전 검사 시간은 이 429.35초에 포함하지 않습니다.

원본·대상 주문 **549,370건**의 최종 비교를 통과했습니다. DMS Start와 Promote가 30초를 넘으면 저장된 실행 ID로 완료를 조회하고 RECONCILE 후 이어갔습니다. 변경 요청을 다시 보내지 않았습니다. 실제 전체 실행은 MIGRATE_ALL SUCCEEDED, API ACTIVE / OK, DMS COMPLETED로 끝났습니다.

별도 SQL 조회로 새 주문이 대상에 존재하고 원본에는 없는 것을 확인했습니다. 원본 writer NOLOGIN, 대상 writer LOGIN, PgBouncer 대상 경로·PAUSE 해제, Cloud SQL의 독립 primary 전환을 확인했습니다. API가 반환한 앱 접속 주소는 유지했습니다. 앱 재개 후 새 주문과 쓰기 오류 0건을 확인했고, 원본·대상 관리 연결의 TLS 1.3도 확인했습니다.

표시 확인 중 Cloud SQL이 `inet_server_addr()::text`를 `/32`가 붙은 주소로 반환하는 것을 발견했습니다. 실제 경로는 대상이었지만 마지막 주문 DB 표시가 원본으로 분류됐습니다. 주소 비교에서 prefix를 제거한 뒤 완료 상태에서 대시보드만 재시작하고, 대상 표시·추가 쓰기를 다시 확인했습니다. 이 대시보드 재시작은 위 마이그레이션 시간 측정 이후입니다.

이번 결과에는 쓰기 정지가 포함됩니다. 실제 무중단, 대상 쓰기 후 롤백, ECS 배포·고가용성을 검증한 결과는 아닙니다. 증거는 Git에서 제외한 `runtime/cloud-start-20261105`의 `execution-result.json`, `execution-timing.json`, `execution-control-proof.json`, `target-active-proof.json`, `target-active.png`에 있습니다.

## 2026-11-05: 처음부터 실행하는 시연

`처음부터 시연 시작` 버튼에서 앱 등록 → 계획·사전 검사 → 초기 복사·CDC → 쓰기 정지 → 대상 전환 → 앱 재개를 순서대로 실행합니다. 중간 단계를 이미 실행했다면 그 상태부터 이어갑니다. 제어 작업 중에도 주문 수·DB 경로를 조회하도록 관측과 앱 쓰기 스레드를 분리했습니다.

단위 검사 61개와 재전환·응답 유실·전환 ID·재시도·차단 조건의 추가 통합 검사 6개가 통과했습니다. 별도 Docker 환경에서는 앱 미등록·대상 0건에서 전체 실행 한 번으로 ACTIVE·대상 새 주문·쓰기 오류 0건을 확인했습니다. 이 전체 경로의 GCP SDK는 모의입니다.

실제 환경은 새 Cloud SQL `foxtrot-demo-start-20261105`와 새 DMS 작업으로 준비했습니다. 기존 완료 시연의 앱 연결은 정상 해제했고, 기존 DB와 데이터를 보존했습니다. 새 세션의 앱 미등록·계획 없음·대상 업무 DB 없음·DMS NOT_STARTED를 확인했습니다. 이후 화면에서 앱 등록·사전 검사를 진행해도 전체 실행은 초기 복사부터 이어갑니다.

새 대상의 TLS 서버 이름 검증과 실제 GCP DMS Verify가 성공했습니다. 이 절은 복사 시작 전의 준비 기록이며, 이후 실제 전환 결과와 시간은 위 절을 따릅니다. 활성 환경 파일은 Git에서 제외한 `runtime/active-cloud.env`입니다. 증거는 `runtime/cloud-start-20261105`의 `ready-verify.json`, `ready-cloud.json`, `initial-flow-smoke.json`, `ready-dashboard.png`에 있습니다.

## 2026-11-05: 실제 쓰기 정지의 확인 기록 갱신

실제 QUIESCE에서 앱 쓰기와 프록시는 정지했지만, 대시보드가 30초 유효한 정지 확인 기록을 갱신하지 않아 `QUIESCE_EVIDENCE_EXPIRED`로 중단됐습니다. 원본과 대상 모두 writer NOLOGIN, PgBouncer PAUSE, 최종 barrier의 양쪽 존재를 확인했습니다.

대시보드에 독립된 5초 갱신 루프를 추가했습니다. 실제 앱 쓰기가 정지했고 진행 중 트랜잭션이 없는 상태에서만 갱신합니다. 실제 실행에서 35초 후에도 갱신이 유지되고 원래 정지 시작 시각이 보존되는 것을 확인했습니다. 300초 전체 정지 제한을 넘긴 전환은 계속 거절합니다.

전수 비교의 FETCH 단위를 100행에서 10,000행으로 바꿨습니다. 실제 원본·대상에서 주문 506,489행, 재고 1,025행, barrier 406행의 digest가 모두 일치했습니다. 측정 시간은 원본 6.156초, 대상 6.027초입니다. 이 수치는 데이터 비교 시간이며 전체 QUIESCE·전환 시간은 아닙니다.

화면은 실행 중에도 저장된 실제 phase와 경과 시간을 조회하며, 시간 제한 초과 시 QUIESCE·CUTOVER를 차단합니다. 시작 시 동기 PgBouncer 조회를 별도 스레드에서 실행해 event loop 충돌도 고쳤습니다.

수정 후 DMS 단위·로컬 통합 검사 49개가 68.92초에 통과했습니다. 실제 클라우드의 전체 QUIESCE·Promote·대상 쓰기를 다시 실행한 결과는 아닙니다.

### 같은 시도의 원본 복귀

300초 제한을 넘긴 시도는 계속 전환하지 않았습니다. 사용자 승인 후 실제 DMS Stop을 실행하고, 공통 API의 RECONCILE → RESUME SOURCE로 앱 쓰기를 재개했습니다. 최종 상태는 RESTORED / OK, DMS STOPPED, 원본 writer LOGIN, 대상 writer NOLOGIN, PgBouncer 원본 경로·PAUSE 해제입니다. 새 주문의 실제 원본 커밋과 쓰기 오류 0건을 확인했습니다.

복귀 중에는 DMS 설정 fingerprint 불일치도 발견했습니다. 대상 접속 프로필의 현재 값은 생성 완료 응답과 비교할 때 출력 필드 `update_time`만 달랐습니다. 기존 fingerprint에는 프로필의 갱신 시간도 포함되므로, 이 관측만으로 과거 모든 설정의 불변을 자동 판정하지 않았습니다.

이번 복귀는 운영자 검토를 포함합니다. 등록된 작업·DB 주소·복제 계정, 대상 쓰기 이력 없음, 양쪽 writer 차단, 원본 schema를 확인했습니다. DMS 정지 실행 ID, 원래 fingerprint, 재검토한 fingerprint와 원본 전수 비교 결과를 별도 `SOURCE_RECOVERY` 기록에 보존했습니다. 복귀 전용 상태를 설정한 뒤 기존 API로 대조·재개했으며, 대상 전환은 다시 허용하지 않았습니다. fingerprint 변경을 자동으로 승인하는 기능을 검증한 결과는 아닙니다.

증거는 Git에서 제외한 `runtime/cloud-dashboard/source-restored-proof.json`과 `source-restored.png`에 있습니다. 대상 데이터와 DB 인스턴스는 유지했습니다. DMS 정지는 DB 인스턴스 정지를 뜻하지 않습니다.

## 2026-11-05: 실제 Cloud SQL 대시보드

실제 RDS `foxtrot-dms-demo-20261104`에서 새 Cloud SQL `foxtrot-dashboard-20261105`로 GCP DMS를 연결했습니다. 대시보드·API·Worker·PgBouncer·DynamoDB Local은 Docker에서 실행합니다. 모의 SDK와 TLS 우회는 사용하지 않습니다.

- 앱 등록과 PgBouncer를 통한 원본 주문 INSERT 성공.
- 공통 API의 PREPARE → 실제 DMS Verify 성공, START_SYNC → 실제 DMS Start 접수. 시작 작업이 30초를 넘으면 UNKNOWN으로 멈추고, 기존 작업 완료 후 RECONCILE로 복구.
- 실제 작업 `RUNNING / CDC`, 공통 API `READY / OK`. 초기 원본 주문 504,530건 이상이 대상에 존재하고 새 주문 10건의 ID·요청 ID·SKU 일치를 별도로 확인.
- RDS·Cloud SQL 직접 접속의 `verify-full`, Cloud SQL TLS 1.3, PgBouncer 컨테이너의 대상 DNS 해석·TLS 서버 이름 검증 성공.
- 빈 대상의 maintenance DB 검사와 이전 Verify 오류 재검사 경로 보완. 관련 검사 49개 통과 후, 마지막 보완의 관련 검사 33개 통과. 서로 겹치는 검사이므로 합산하지 않음.
- 브라우저에서 실제 리소스·상태·주문 수 표시 확인. 초기화·모의 장애 버튼 숨김. 좁은 화면의 가로 넘침과 JavaScript 오류 없음.

이 절은 최초 READY 도달 시점의 기록입니다. 이후 원본 복귀 결과는 위 절을 따릅니다. 실제 Promote·대상 전환은 실행하지 않았습니다. 기존 Cloud SQL의 데이터와 NEVER 정책은 보존했습니다. RDS와 새 Cloud SQL 인스턴스는 실행 중이며, Docker 또는 DMS 정지만으로 DB 과금이 끝나지 않습니다. 증거는 Git에서 제외한 `runtime/cloud-dashboard`의 `cdc-proof.json`, `final-dashboard.json`, `target-permissions.json`, 회귀 검사 XML과 화면 캡처에 있습니다.

## 2026-11-05: GCP DMS와 공통 API 연결

공통 API에 `gcp-dms-postgres-pgbouncer-v1`을 추가했습니다. 복사·CDC·승격은 GCP DMS에 요청하고, Worker가 결과를 확인한 뒤 PgBouncer 경로를 바꿉니다. DMS 실패 시 자체 복제로 전환하지 않습니다. 설정과 실행 순서는 [DMS API 가이드](dms-api.md)에 있습니다.

전체 회귀 검사 **172개가 467.58초에 통과했습니다.** 단위 56개·통합 116개이며 실패·오류·건너뛴 검사는 없습니다. 이 결과는 아래 실제 프로필 형식 보완 전의 회귀 검사입니다.

이후 실제 GCP 설정 조회에서 대상 프로필이 `cloudsql` 생성 설정 대신 `postgresql.cloud_sql_id`를 쓰는 것을 확인했습니다. 두 형식을 처리하고 Cloud SQL 관리 API의 GET으로 대상 인스턴스·주소를 대조하도록 보완했습니다. 최종 소스로 **DMS 관련 검사 65개가 60.14초에 통과했습니다.** 이 65개는 회귀 검사와 일부 겹치므로 합산하지 않습니다.

| 검사 | 확인한 동작 |
|---|---|
| 실제 API·SQL·PgBouncer | 등록 → DMS Verify·Start → READY → 정지 → Promote 완료 → 대상 전환·새 쓰기. 앱 endpoint 유지 |
| 권한·sequence | 대상의 기존 table·sequence 권한을 없앤 뒤 전환. 최소 업무 권한과 sequence 준비 후 새 주문 INSERT 성공 |
| 원본 복귀 | 대상 쓰기 전 DMS Stop → 원본 복귀 → 새 조회. 미사용 대상 DB의 SQL 접속 장애 중에도 원본 복귀 |
| 응답 유실 | 승격 접수 응답 유실 시 UNKNOWN. 정확한 클라우드 실행 ID를 연결한 뒤 이어감. Promote 중복 호출 없음 |
| 승격 미완료 | RPC 성공이어도 작업이 COMPLETED가 아니면 대상 쓰기 차단 |
| 재개 중단 | 대상 LOGIN 전·후 중단을 대조하고 같은 대상의 RESUME만 허용. 원본 롤백 금지 |
| 인스턴스 범위 | 같은 클라우드 인스턴스의 다른 binding이 있으면 전체 승격 계획 거절 |
| 최종 확인 유실 | FINALIZE 확인 실패 시 자원 점유 유지. RECONCILE 성공과 같은 트랜잭션에서 반환 |
| 이력·재전송 | GCP 인증 없이 완료 이력·해제 후 대조·동일 명령 응답 조회. API가 점유한 작업의 별도 CLI 변경 거절 |
| 대상 조회 형식 | 기존 Cloud SQL 프로필의 instance ID 조회, 잘못된 instance 거절, 주소 변경 시 fingerprint 변경, OUTGOING 주소 제외 |

GCP SDK는 모의 응답을 사용하고 DB·PgBouncer·DynamoDB는 로컬 실물을 사용했습니다. SQL 준비 검사는 실행했습니다. 공유 랩의 주소·전용 인스턴스 조건과 비암호화 DB transport는 시험용 어댑터로 대체했으며 주소·범위·TLS 거절 조건은 별도 단위 검사로 확인했습니다. RDS·Cloud SQL의 실제 SQL 권한 검증을 대신하는 결과는 아닙니다.

실제 클라우드에서는 기존 완료 DMS 작업·연결 프로필·Cloud SQL 인스턴스 메타데이터만 조회했습니다. 새 형식에서 대상 instance ID·접속 주소가 정상 해석됐습니다. Cloud SQL의 activationPolicy는 NEVER였습니다. 이번 작업에서 클라우드 DB 시작·DMS Start·Promote·삭제는 실행하지 않았습니다.

최종 Ruff·포맷 검사와 wheel·sdist·Docker 빌드가 통과했습니다. ECS 인증 JSON은 backend에만 주입하는 설정과 0600 파일 생성을 확인했습니다. 구현 Python 파일 수는 기존과 같은 13개입니다. 실제 ECS·IAM·GCP 인증 배포·공인망 DB TLS·새 DMS 전환 시험은 남아 있습니다. 대상 쓰기 후 역복제·롤백은 구성하지 않았으며 해당 요청을 거절합니다.

기록은 Git에서 제외한 runtime/dms-api-work의 final.xml, compatibility.xml, live-metadata-final.json에 있습니다.


## 2026-11-05: 여러 앱의 등록·해제

전체 검사 **143개가 통과했습니다.** 단위 검사 43개와 로컬 통합 검사 100개입니다. 실패·오류·건너뛴 검사는 없으며 실행 시간은 435.22초입니다. 새 binding 통합 검사 12개를 포함합니다.

| 검사 | 확인한 결과 |
|---|---|
| 두 앱 등록·접근 분리 | 서로 다른 DB·schema를 한 PgBouncer에서 사용. 유효한 A 계정으로 B alias 접속 거절 |
| 해제 범위 | A 해제 중 B의 기존 연결과 새 쓰기 유지. 공유 DB는 앱 관계만 해제 |
| 실제 연결 종료 | 클라이언트가 남으면 UNKNOWN에서 중단. 연결 종료·RECONCILE·새 근거·RETRY 뒤 제거 |
| 운영을 유지하는 해제 | 현재 DB에 직접 연결한 업무 계정으로 해제 전후 쓰기 성공. DB·데이터·LOGIN 역할 유지 |
| 근거·경합 | 만료·revision 변경·마이그레이션 전의 해제 근거 거절. 진행 중 이전의 해제 차단 |
| 실패 복구 | 완료 응답 유실 대조, 초기 등록 실패 취소·재등록, 한 binding의 복원 실패 격리 |
| 기존 이전 연결 | 동적 binding으로 초기 복사·CDC·전환·새 쓰기·역방향 롤백·FINALIZE. 다른 앱의 연결 유지 |
| 완료 이력 | 해제 후 GET과 명시적 RECONCILE도 없어진 DB 접속점에 접근하지 않음. 기존 완료 후 RECONCILE 계약 유지 |

최종 Docker 이미지에서 backend(API·Worker)와 PgBouncer를 실제 컨테이너로 실행했습니다. UID 10001, 읽기 전용 루트, capability 제거, 공유 설정 볼륨을 적용했습니다. 클라이언트 TLS 인증서와 호스트 이름을 검증했습니다.

두 앱 등록 → A만 해제 → Task 구성 재시작 → B 추가 쓰기를 확인했습니다. 이어 B의 실제 PostgreSQL 논리 복제를 실행하고 HELD 재시작, 대상 전환 후 OPEN 재시작, 역방향 롤백을 확인했습니다. 최종 주문 6건·재고 994·sequence 증가를 확인한 뒤 FINALIZE·해제·재시작했습니다. 해제한 alias는 복원되지 않았습니다.

Docker 시험에서 PgBouncer가 접속 검사에 사용한 startup `options`를 거절하는 문제를 찾았습니다. 검사 연결에서 이 옵션을 제거하고 트랜잭션 안의 `SET LOCAL statement_timeout`으로 바꿨습니다. 새 통합 fixture에서도 startup options를 허용하지 않아 최종 이미지와 같은 조건으로 검사합니다.

Ruff 검사·포맷 검사와 wheel·sdist·Docker 빌드가 통과했습니다. wheel과 이미지의 Python 구현 파일 13개 및 contract.json이 현재 소스와 바이트 단위로 같습니다. sdist에 runtime 자격 증명이 포함되지 않았습니다. ECS 생성 JSON의 두 컨테이너·512 CPU 단위·1024 MiB 합계·동적 등록 설정·앱별 DSN 환경 변수 주입 제외도 검사했습니다.

이번 검증은 자체 운영 PostgreSQL 18·PgBouncer 1.25.1·DynamoDB Local입니다. Docker 시험의 Secrets Manager는 boto3의 전송 경로를 로컬 HTTP 모의 서버로 바꿨습니다. 실제 AWS IAM·Secrets Manager·ECS·공인망을 검증한 결과는 아닙니다. DB 측 TLS는 시험용으로 비활성화했습니다. 기존 성능 리포트는 정적 Registry 구성에서 측정한 값입니다.

원본 기록은 Git에서 제외한 runtime/bindings-final.xml, runtime/bindings-runtime-smoke.log, runtime/bindings-package.json에 있습니다. 사용법은 [앱 등록·해제 가이드](bindings.md)를 따릅니다.


현재 구성의 결과는 [동작·성능 검증 리포트](performance-report.md)에 정리했습니다. 아래 기록은 변경 시점별 결과를 보존합니다.

## 2026-11-05: 동시 조회 수정과 성능 측정

예비 부하 검사에서 상태 조회가 겹칠 때 HTTP 409가 발생했습니다. 캐시 만료 뒤 여러 요청이 관측 결과를 동시에 저장한 것이 원인입니다. 같은 API 프로세스의 관측 갱신을 묶고, 대기한 요청은 갱신된 캐시를 다시 읽도록 바꿨습니다. 단위 검사 43개와 동시 조회·캐시·관측 실패·상태 경합·명령 성공 보존의 통합 검사 5개가 통과했습니다.

CPU를 작게 제한한 시험에서는 기존 Python 상태 검사도 시간 초과가 났습니다. ECS 상태 검사를 PgBouncer의 `pg_isready`와 backend의 `curl /live`로 바꿨습니다. 변경 후 같은 제한에서 두 컨테이너가 정상 시작했고 아래 부하·전환·롤백 검사를 마쳤습니다.

| 로컬 구성 | 상태 조회/초 | 조회 P95 | 주문 트랜잭션/초 | 주문 P95 | 오류 |
|---|---:|---:|---:|---:|---:|
| API·Worker 분리, CPU 제한 없음 | 370.63 | 31.59ms | 2,778.06 | 8.81ms | 0 |
| API·Worker 통합, CPU 제한 없음 | 402.94 | 29.69ms | 2,872.59 | 8.43ms | 0 |
| 통합, 합계 0.5 vCPU·1 GiB 제한 | 278.03 | 78.24ms | 1,471.01 | 62.47ms | 0 |

각 값은 6초씩 3회 측정한 중앙값입니다. 상태 조회 8개와 주문 DB 연결 16개가 동시에 계속 요청했습니다. 원본 주문 5만 건·상품 128개로 시작했습니다. 주문은 임의 상품의 재고 감소와 고유 주문 INSERT를 하나의 트랜잭션으로 실행했습니다. 클라이언트 TLS와 실제 PostgreSQL 논리 복제를 사용했습니다. API 값은 관리용 상태 조회이며, 주문 값은 HTTP 앱을 거치지 않은 DB TPS입니다.

첫 두 구성은 같은 기존 Python 상태 검사 조건입니다. 제한 구성은 개선한 네이티브 상태 검사를 10초 간격으로 사용합니다. backend에 0.375 CPU·768 MiB, PgBouncer에 0.125 CPU·256 MiB를 강제 적용했습니다. PostgreSQL·DynamoDB Local과 부하 생성기는 이 한도 밖에 있습니다. 이 구성의 컨테이너 메모리 합계는 측정 중 최대 표본값 249.44 MiB였습니다. 실제 최대 메모리나 장시간 용량 상한을 보장하는 값은 아닙니다.

제한 구성에서 초기 계획 등록·PREPARE·START_SYNC 후 READY까지 1.80초가 걸렸습니다. 업무를 정지한 뒤 QUIESCE 준비부터 대상 첫 쓰기까지 3.52초, 역방향 롤백 후 원본 첫 쓰기까지 3.89초였습니다. 검증한 최종 주문은 76,465건이며 고유 주문 수·재고 합계·sequence가 일치했습니다.

CPU 제한 없는 두 실험에서는 같은 시간 동안 변경이 더 많이 발생했습니다. 전환 절차는 각각 20.89초와 19.63초였습니다. 변경량이 달라 이 시간을 단순한 컨테이너 병합 효과로 비교할 수 없습니다. 전환 중 쓰기 정지가 있으므로 무중단 쓰기 결과로 해석하면 안 됩니다.

환경은 Apple M4·OrbStack ARM64 Docker·DynamoDB Local·PostgreSQL 18입니다. 짧은 폐쇄형 부하 검사이며 최대 처리량·100,000 RPS·실제 ECS·AWS↔GCP 네트워크·클라우드 DMS 성능은 검증하지 않았습니다. 원본 측정값과 실행 스크립트는 Git에서 제외한 runtime/backend-performance에 있습니다.

## 2026-11-05: API·Worker 컨테이너 통합

ECS 구성을 backend와 PgBouncer, 컨테이너 두 개로 바꿨습니다. backend의 기본 명령이 API·Worker 프로세스를 함께 실행합니다.

단위 검사 43개, Ruff 검사·포맷 검사, wheel·sdist·Docker 빌드가 통과했습니다. wheel과 이미지의 Python 파일 13개 및 contract.json이 현재 소스와 같습니다. ECS 등록 JSON의 두 컨테이너, 시작 순서, 상태 검사, 공유 볼륨과 CPU·메모리 합계를 검사했습니다.

실제 Docker 컨테이너에서 다음 동작을 확인했습니다.

| 검사 | 결과 |
|---|---|
| API 강제 종료 | 실행기가 Worker도 정상 종료하고 backend를 종료 코드 1로 정지. Worker 잠금 해제 확인 |
| Worker 강제 종료 | 실행기가 API를 종료하고 backend를 종료 코드 1로 정지. Worker 잠금은 보존. 컨테이너 종료와 미접수 상태를 확인한 뒤 정확한 소유자로 수동 해제 |
| backend SIGTERM | API·Worker 종료, 잠금 해제, 컨테이너 종료 코드 0 확인 |
| 초기 복사·전환 | API 접수 후 같은 컨테이너의 Worker가 실행. 실제 대상 DB에서 추가 쓰기 성공 |
| 대상 운영 중 재시작 | TARGET·OPEN 경로 복원 |
| 업무 정지 중 재시작 | HELD·PAUSE와 양쪽 writer 차단 유지 |
| 역방향 롤백 | 원본 복귀 후 추가 쓰기·FINALIZE 성공. 주문 3건·재고 997·sequence 증가 확인 |

시험은 DynamoDB Local·자체 운영 PostgreSQL 18을 사용했습니다. 클라이언트 TLS를 검증했고 DB 측 연결은 시험용 비암호화 연결이었습니다. 실제 ECS 배포·IAM·공인망 검증은 수행하지 않았습니다. 이번 변경 뒤 전체 130개 검사를 다시 실행하지는 않았습니다. 아래 130개 결과는 컨테이너 통합 전 DynamoDB 전환 기록입니다.

원본 결과는 Git에서 제외한 runtime/backend-verification에 보관합니다.

## 2026-11-05: DynamoDB 전환

**전체 검사 130개가 통과했습니다.** 실패·오류·건너뛴 검사는 없습니다. 실행 시간은 406.22초입니다. 로컬 작업 트리의 결과이며 실제 클라우드나 원격 CI의 결과는 아닙니다.

| 검사 | 확인한 내용 |
|---|---|
| 상태 저장 | DynamoDB Local에서 조건부 트랜잭션·충돌 시 부분 반영 금지·Query 중 추가된 항목 감지 |
| 동시 접수 | 같은 멱등 키 12건을 동시에 접수해 계획 1개와 자원 점유 1세트만 생성. revision 불일치 명령 거절 |
| 실행기 잠금 | 시간 경과만으로 인계 금지. 정확한 소유자와 종료 근거 요구. 같은 프로세스의 잠금 응답 유실 복구 |
| 정지 근거 | 별도 control namespace, generation·만료·구성 버전 검사, 이름에 구분 문자가 있는 업무 범위의 조회 분리 |
| DB 이전 | 실제 PostgreSQL 18·PgBouncer에서 초기 복사·CDC·전환·대상 새 쓰기·역방향 롤백·sequence 보존 |
| 실패 복구 | 외부 변경 의도와 HELD 경로의 원자적 저장, 중단 작업 UNKNOWN 보존, 부분 정지·재개와 명시적 복구 |
| DMS 기록 | 실제 DynamoDB Local과 모의 Google SDK로 멱등 접수·설정 변경·접수 응답 유실·실행 근거 대조 |

DynamoDB 안전성 검사 10개를 포함합니다. 마지막 변수 이름 정리 후 저장소·DMS 관련 검사 40개도 다시 통과했습니다. 이 40개는 전체 검사와 겹치므로 총 검사 수에 더하지 않습니다.

### 세 컨테이너 실행 확인

Docker에서 API·Worker·PgBouncer를 별도 컨테이너로 실행했습니다. 네트워크와 설정 볼륨을 공유하고, UID 10001·읽기 전용 루트 파일 시스템·capability 제거 조건을 적용했습니다.

1. API로 PREPARE → START_SYNC → QUIESCE → CUTOVER → RESUME TARGET을 요청했습니다. 실제 Worker가 실행했습니다.
2. 대상에서 새 주문을 기록하고 세 컨테이너를 재시작했습니다. 저장된 TARGET·OPEN 경로가 복원됐습니다.
3. 업무를 정지하고 다시 재시작했습니다. HELD·PAUSE와 양쪽 writer 차단이 유지됐습니다.
4. ROLLBACK → RESUME SOURCE 후 새 주문을 기록하고 FINALIZE했습니다. 주문 3건, 재고 997, 시퀀스 증가를 확인했습니다.

클라이언트 TLS 인증서와 호스트 이름을 검증했습니다. 이 로컬 시험의 DB 간 연결에는 TLS를 사용하지 않았습니다. 공인망과 DB 측 TLS 검증을 대신하는 결과는 아닙니다.

### 빌드와 정리

Ruff 검사·포맷 검사, wheel·sdist 빌드, Docker 이미지 빌드가 통과했습니다. wheel과 Docker 이미지의 Python 파일 13개 및 contract.json이 현재 소스와 같은지 바이트 단위로 확인했습니다. ECS JSON은 단일 Task·컨테이너 3개·공인 IP·DynamoDB 설정을 검사했습니다. 테이블 템플릿에는 온디맨드·PITR·삭제 방지가 포함됩니다.

검증용 Compose 프로젝트의 컨테이너와 볼륨을 제거했습니다. 기존 알림 서비스 컨테이너는 유지했습니다. 클라우드 DB를 시작하거나 변경하지 않았습니다.

현재 검증은 자체 운영 PostgreSQL 프로필에 한정합니다. 실제 ECS·IAM·공인망 배포는 미검증입니다. 공개 DNS·허용 IP 자동 갱신과 DMS 컷오버·롤백의 공통 API 연결은 구현 범위에 포함되지 않습니다. 기존 PostgreSQL 관리 상태의 자동 이관도 제공하지 않습니다.

재현 방법은 [README](../README.md#로컬-시험), 배포 조건은 [ECS 실행 구성](../deploy/ecs/README.md)에 있습니다. 원본 결과는 Git에서 제외한 runtime/dynamodb-verification에 보관합니다.

---

## 이전 검증 기록: 2026-11-04

아래는 관리용 PostgreSQL을 사용하던 구현의 기록입니다. 현재 DynamoDB 검증 결과와 구분합니다.

검증일: 2026-11-04. 실제 클라우드 검증 기록은 아닙니다.

| 환경 | 값 |
| --- | --- |
| Python | 3.13.18 |
| uv | 0.13.6 |
| PostgreSQL | 18.6, 원본·대상·별도 관리 DB |
| PgBouncer | 1.25.1 |
| 실행 환경 | macOS + OrbStack, 별도 Compose 프로젝트 |
| 데이터 | inventory·orders, PK·identity sequence, 시험용 주문 |

## 초기 구현 결과

2e85f4a 기준으로 21개 테스트가 통과했습니다. 모델 검사 14개와 실제 DB·프록시를 사용하는 통합 검사 7개입니다. 관리 DB는 superuser가 아닌 별도 역할로 검사했습니다.

| 통합 검사 | 확인한 결과 |
| --- | --- |
| 초기 복사·전환·역방향 롤백 | 대상에서 새로 쓴 주문이 원본 복귀 후에도 존재. 시퀀스 충돌 없이 추가 쓰기 성공 |
| RESUME 응답 유실 | 실제 재개 후 오류를 주입. 대상 쓰기 가능성이 true로 보존되고 UNKNOWN에서 변경 차단. 새 API·Worker 인스턴스가 RECONCILE로 대조 |
| 대상 쓰기 전 복귀 | 원본 경로로 복귀하되 별도 RESUME 전까지 업무 차단 |
| 역복제 없는 대상 새 쓰기 | 원본 자동 복귀와 ROLLBACK 거절. 대상 단독 운영 유지 |
| 실행기·멱등 접수 | 같은 요청의 동시 접수 결과가 하나. Worker 잠금 중복 획득 거절. RUNNING 기록 재시작 시 재실행 금지 |
| 이전 실행 연결·잠금 유실 | idle인 이전 관리 연결도 복구 완료 판정을 차단. 늦은 종료 기록이 새 명령을 덮어쓰지 못함 |
| 정지 시간 상한 초과 | 불완전한 미사용 대상이 있어도 원본을 검증해 복귀. 역방향 검증 중단 후에는 새 QUIESCE로 재검증해야 원본 복귀 가능 |

만료·위조된 정지 근거, 다른 요청 본문에 같은 키 사용, 업무 역할을 우회하는 PgBouncer force_user도 검사합니다. 실제 HTTP 응답은 공통 OpenAPI 0.2 스키마에 대조했습니다.

Ruff 검사·포맷 검사와 wheel·sdist 빌드도 통과했습니다. 만든 wheel을 개발 의존성이 없는 별도 환경에 설치해 API·Worker 프로세스를 실행했습니다. /health, /docs, /redoc, /openapi.json, 인증된 /v1/capabilities가 응답했고 미인증 요청은 401이었습니다. 프로세스 종료와 API 포트 폐쇄를 확인했습니다.

## 검토 반영 후 검사

설정 파서 단위 검사 6개와 회귀 통합 검사 11개를 추가했습니다. 전체는 단위 검사 20개와 통합 검사 18개, 총 38개입니다. 기존 7개 통합 검사도 포함합니다. 수정 내역은 [장애 경로 검토와 수정](review-fixes.md)에 있습니다.

첫 검토 수정본에서 38개 모두 통과했습니다. JUnit 기준 실행 시간은 90.35초이며 실패·오류·건너뛴 검사는 없습니다. 랩의 원본·대상·관리 DB를 정지했고 볼륨은 보존했습니다.

| 추가 검사 | 확인한 내용 |
| --- | --- |
| 설정 보존 | CRLF·주석·%include·인증·pool 옵션 보존. 실제 PgBouncer에서 전환·복귀와 RELOAD 수행 |
| 관측 실패 | 일시 실패는 다음 GET에서 회복. 실제 명령 UNKNOWN은 그대로 유지. GET·POST 관리 DB 오류 응답 구분 |
| 조회 캐시·잠금 | 원격 DB 접속 없이 캐시 조회. 다른 연결이 행 잠금을 잡아도 캐시 GET 완료 |
| 상태 경합 | 오래된 READY/SYNCING 관측과 실제 QUIESCE를 경합시켜 최신 상태 보존 |
| 성공 후 관측 예외 | 성공 기록 유지. 같은 Worker가 다음 작업을 처리할 수 있음 |
| 직접 롤백 중단 | 대상 데이터가 불완전해도 경로 변경 전·후 예외에서 원본 기준 RECONCILE 수행 |
| 대상 DB 정지 | 최종 동기화 도중·CUTOVER 뒤에 대상 컨테이너를 실제 정지. 쓰기 허용 전에는 원본 복귀와 새 쓰기 성공 |
| 대상 쓰기 후 장애 | 저장된 초기 차단 근거를 재사용하지 않음. 대상 장애 중 원본 복귀 차단. 재기동·역복제 재연결 후 검증·복귀 |
| LSN 비교 | 과거의 NULL이 아닌 LSN을 실제 검사 SQL에 주입. 경계 미도달을 거절하고 이후 실제 진행 LSN 확인 |

새 wheel의 Python 모듈이 현재 소스와 같은지 검사했고, 프로젝트 밖 격리 환경에서 CLI를 실행했습니다. 수정한 보고서도 브라우저로 확인했습니다. 원격 CI 결과와는 구분합니다.

## 이 기록의 범위

- 응답 유실·검증 중단은 예외 주입을 사용합니다. 추가 회귀 검사에는 로컬 대상 DB 컨테이너의 정상 정지·재기동도 포함합니다. 실제 클라우드 네트워크 단절·호스트 전원 상실 시험은 수행하지 않았습니다.
- 큰 DB의 복사 시간·검증 시간·WAL 보관량·부하·정지 시간 상한은 측정하지 않았습니다. 30초 안정 구간 통과는 성능 보장을 뜻하지 않습니다.
- 실제 앱의 정지 채널은 오케스트레이터가 구현해야 합니다. 이 통합 환경은 제어할 수 있는 시험 주체만 등록합니다.
- RDS·Cloud SQL·실제 온프레미스 네트워크, 관리형 권한, 프록시 고가용성은 검증하지 않았습니다.
- GitHub Actions 설정은 추가했지만 원격 CI 실행 결과는 아닙니다.

## 2차 독립 검토 반영 후 최종 검사

2026-11-04, 2e85f4a 기반 fix/review-recovery 작업 트리에서 **73개 모두 통과**했습니다. 단위 검사 26개와 통합 검사 47개입니다. 실패·오류·건너뛴 검사는 없습니다. JUnit 기준 실행 시간은 397.95초입니다. 원격 main이나 CI의 결과는 아닙니다.

| 보강한 검사 | 확인한 내용 |
| --- | --- |
| 부분 정지·재개 | 원본·대상 PAUSE 뒤 차단 전 중단, 대상 LOGIN 전·후 중단, 반복 RECONCILE, 실제 writer 세션 종료 후 재개 |
| 복귀 전용 정지 | 정지 기한 초과, 같은 복구 시도의 재중단, 미사용 대상 연결 장애가 겹쳐도 원본 복귀. CUTOVER는 거절 |
| FINALIZE | 새 HELD·실패 probe·동시 접수로 오래된 승인을 차단. 실제 정방향 구독 삭제 뒤 중단해도 명시적 재시도로 정리 완료 |
| 제어 근거 | 30회 갱신의 TTL 상한, 새 generation의 안정화 초기화, 잘못된 참여자 근거 뒤 정상 heartbeat, 정상 요청 수 변화 |
| 실제 안정화 | 30초 이상 실제 대기와 5초 heartbeat 후 진행 PASS·FINALIZE 성공. 다른 빠른 검사는 경과 시각을 주입한 경우를 구분 |
| schema 기준값 | DEFAULT replica identity 요구, UNIQUE INDEX 누락 차단, USING이 들어간 서로 다른 index 이름 허용, sequence 범위 차이와 준비 후 설정 변경 차단 |
| 설정 권한 | macOS의 실제 그룹·mode·확장 속성 보존과 확장 ACL 거절. 별도 Linux 컨테이너에서 root 작성자와 UID/GID 65534 PgBouncer를 분리해 0640 설정 RELOAD·실제 경로 반영 확인 |

Ruff 검사·형식 검사와 wheel·sdist 빌드를 통과했습니다. wheel의 Python 파일 12개가 현재 소스와 바이트 단위로 같은지 확인했습니다. 별도 검증 에이전트 3명이 CDC, 제어 경합, 복구 경로를 나눠 다시 검토했습니다. 지적한 추가 경계도 수정하고 검사에 포함했습니다.

각 통합 검사는 자기 시험 DB의 복제 객체만 정리합니다. 시험 데이터·DB·볼륨은 보존합니다. 실제 클라우드, 대용량 부하, 호스트 전원 상실, 실행기·프록시 고가용성은 이번에도 검증하지 않았습니다.

최종 검사 후 랩 컨테이너 4개를 정지했습니다. 등록한 로컬 포트 4개의 폐쇄와 데이터 볼륨 3개의 보존을 확인했습니다. 문서 서버는 종료하지 않았습니다.
