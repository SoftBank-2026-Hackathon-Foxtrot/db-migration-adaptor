# ECS 실행 구성

Fargate Task 하나에서 backend와 PgBouncer, 두 컨테이너를 실행합니다. backend 안에 API·Worker 프로세스를 함께 둡니다. PgBouncer는 항상 한 개입니다. TCP 로드밸런서는 사용하지 않습니다. 현재 Task당 binding 하나를 지원합니다.

```text
앱 · GCP · 온프레미스 → 공개 DNS:6432 → PgBouncer → 선택한 DB
AWS 오케스트레이터   → 사설 DNS:8080 → API

단일 Fargate Task
├─ pooler: 저장 경로 복원 → PgBouncer 실행
└─ backend
   ├─ API: 명령 접수 → DynamoDB
   └─ Worker: DynamoDB 작업 조회 → 실제 DB·PgBouncer 변경
   공유 볼륨: /run/foxtrot
```

## 준비할 것

- ECS 클러스터, 퍼블릭 서브넷, 인터넷 게이트웨이 경로, Security Group, ECR, CloudWatch 로그 그룹.
- DynamoDB 테이블. partition key는 `namespace` 문자열, sort key는 `record` 문자열입니다.
- API·Worker·PgBouncer가 공유할 고정 `state_namespace`. 같은 DB·프록시를 제어하는 프로세스는 모두 같은 값을 사용해야 합니다. 서로 다른 namespace가 같은 자원을 제어하는 구성은 지원하지 않습니다.
- Cloud Map 사설 DNS. AWS 내부 API 접속에 사용합니다.
- 공개 DNS와 Task 공인 IP 갱신 절차. Cloud Map은 Task의 사설 IP를 등록하므로 공개 DNS를 대체하지 않습니다. 현재 코드에는 공개 DNS·방화벽 허용 IP 자동 갱신이 없습니다.
- 원본·대상 DB에 접속할 경로. Cloud SQL과 온프렘 DB는 공인 주소·허용 IP·TLS를 설정합니다. 온프렘 공유기 뒤에 DB가 있으면 포트포워딩이 필요합니다.

`6432`는 앱의 허용된 송신 IP에서만, `8080`은 AWS 내부 오케스트레이터에서만 허용합니다. PgBouncer 관리 계정은 같은 Task의 loopback 접속만 허용합니다.

`assign_public_ip=false`로 사설 배포도 선택할 수 있습니다. 이 경우 AWS 외부 API에 접속할 NAT 또는 별도 경로가 필요합니다.

## DynamoDB 권한

`aws-iam` 기준으로 실행 역할과 Task Role을 구분합니다. 실행 역할은 ECR·로그·Secret 읽기를 맡고, Task Role은 DynamoDB 상태를 읽고 씁니다. 정적 AWS access key를 Secret에 넣지 않습니다.

| 주체 | 상태 namespace | `<namespace>:control` |
|---|---|---|
| API·Worker·PgBouncer Task Role | 읽기·쓰기 | 읽기 |
| 오케스트레이터의 별도 역할 | 읽기·쓰기 | 읽기·쓰기 |

일반 실행에 필요한 데이터 작업은 `GetItem`, `Query`, `PutItem`, `UpdateItem`, `DeleteItem`, `ConditionCheckItem`입니다. `DescribeTable`도 필요합니다. 트랜잭션 IAM 권한은 개별 데이터 작업으로 검사합니다. 테이블 ARN과 `dynamodb:LeadingKeys`로 범위를 제한하고, `ForAllValues` 조건에는 같은 키의 `Null=false` 검사도 넣습니다. 배포·테이블 생성 권한은 Task Role에 주지 않습니다.

IAM 기본 권한 분석은 다음 명령으로 재현할 수 있습니다. 출력은 기준 목록입니다. 생성·복제·KMS 등 일반 Task 실행에 필요 없는 권한을 제거하고, 위 namespace 범위를 적용한 뒤 IAM Simulator로 검증해야 합니다. 생성 결과를 그대로 연결하지 않습니다.

```sh
DISABLE_IAM_POLICY_AUTOPILOT_TELEMETRY=true \
uvx iam-policy-autopilot@latest generate-policies \
  "$PWD/src/db_operator/store.py" --region ap-northeast-2 --service-hints dynamodb --pretty
```

DynamoDB Local은 실제 IAM 정책을 검사하지 않습니다. 로컬 검사 통과가 IAM 검증을 뜻하지 않습니다.

## 환경 변수와 Secret

일반 환경 변수:

- `DB_OPERATOR_STATE_TABLE`: 상태 테이블 이름.
- `DB_OPERATOR_STATE_NAMESPACE`: 해당 앱·운영자의 고정 상태 영역.
- `AWS_DEFAULT_REGION`: 테이블이 있는 리전.
- `DB_OPERATOR_REGISTRY`: `/run/foxtrot/registry.json`.

`DB_OPERATOR_DYNAMODB_ENDPOINT`는 로컬 DynamoDB에서만 사용합니다. ECS 설정에는 넣지 않습니다. `DB_OPERATOR_MANAGEMENT_DSN`은 제거했습니다.

Secrets Manager Secret에는 다음 키를 저장합니다.

| 키 | 값 |
|---|---|
| `DB_OPERATOR_API_TOKEN` | 오케스트레이터의 Bearer 토큰. 24자 이상 |
| `DB_OPERATOR_REGISTRY_JSON` | `registry.example.json`을 JSON 문자열로 저장 |
| `DB_OPERATOR_RUNTIME_USERS_JSON` | PgBouncer 관리·업무 계정 비밀번호의 JSON 문자열 |
| `DB_OPERATOR_RUNTIME_CLIENT_CERTIFICATE_PEM` | PgBouncer·API TLS 인증서 체인 |
| `DB_OPERATOR_RUNTIME_CLIENT_KEY_PEM` | 해당 개인 키 |
| `DB_OPERATOR_RUNTIME_SERVER_CA_PEM` | DB·프록시 인증서를 검증할 CA 묶음 |
| `SOURCE_ADMIN_DSN`, `TARGET_ADMIN_DSN` | 실제 DB 관리 DSN. `sslmode=verify-full` 사용 |
| `SOURCE_REPLICATION_DSN`, `TARGET_REPLICATION_DSN` | 실제 DB의 복제 DSN |
| `PGBOUNCER_ADMIN_DSN` | loopback `127.0.0.1:6432`, DB `pgbouncer`, 사용자 `pooler_admin` |
| `PGBOUNCER_PROBE_DSN` | loopback `127.0.0.1:6432`, 업무 alias·업무 사용자 |

CA 경로는 `/run/foxtrot/server-ca.crt`입니다. 프록시 인증서에는 `127.0.0.1` IP SAN, 앱의 공개 DNS 이름, 내부 API DNS 이름을 넣습니다. 외부 앱은 CA와 호스트 이름을 모두 검증합니다.

업무 비밀번호는 원본·대상 writer의 비밀번호와 같아야 합니다. 첫 등록은 원본 writer만 `LOGIN`, 대상 writer는 `NOLOGIN`인 상태에서 수행합니다. 복제 DSN의 CA 파일은 실제 PostgreSQL 서버에도 따로 배치해야 합니다.

## 빌드와 등록 파일

```sh
docker build -t foxtrot/db-operator:ecs-local .
uv run --locked python scripts/render_ecs.py \
  --config runtime/ecs-config.json --output runtime/ecs
```

`example.json`을 복사해 실제 이미지, 역할, 테이블, namespace, 서브넷, 보안 그룹, Secret, Cloud Map ARN을 채웁니다. 이미지 아키텍처와 `cpu_architecture`를 맞추고 배포 이미지는 digest로 고정합니다.

생성 파일은 세 개입니다.

- `task-definition.json`: Fargate 0.5 vCPU / 1 GiB, 컨테이너 2개, Secret, 공유 볼륨, 상태 검사.
- `service.json`: Task 1개, 공인 IP 기본 활성화, 기존 Task 종료 후 새 Task 실행.
- `state-table.json`: DynamoDB CloudFormation 템플릿. 온디맨드, PITR, 삭제 방지, 리소스 보존 설정.

생성 명령은 AWS에 리소스를 만들지 않습니다. 테이블을 먼저 배포하고 Task 정의를 등록한 뒤, 서비스 JSON의 `taskDefinition`을 등록 응답의 revision 포함 ARN으로 바꿉니다. 새 서비스용 JSON을 기존 서비스의 UpdateService에 그대로 보내지 않습니다.

컨테이너 사용자는 `10001:10001`이고 루트 파일 시스템은 읽기 전용입니다. `/run/foxtrot`과 `/tmp`만 쓸 수 있습니다. backend의 Worker가 공유 볼륨의 PgBouncer 설정을 바꿉니다.

backend의 기본 명령은 `foxtrot-db-runtime backend`입니다. 실행기가 API와 Worker를 별도 프로세스로 시작합니다. 하나가 종료되면 나머지도 종료하고 컨테이너를 오류로 끝냅니다. SIGTERM을 받으면 두 프로세스를 정상 종료합니다. 최대 105초를 기다린 뒤 남은 프로세스를 강제 종료합니다. Worker가 강제 종료됐으면 아래 잠금 복구 절차가 필요합니다.

## 시작과 복원

1. Secret에서 Registry·인증·TLS 파일을 준비합니다.
2. DynamoDB 테이블과 namespace를 확인하고 실행기 잠금을 얻습니다.
3. 중단된 RUNNING 작업을 자동 재실행하지 않고 FAILED 또는 UNKNOWN으로 판정합니다.
4. 최초 배포에서만 초기 경로를 등록합니다. 이후에는 저장한 경로를 사용합니다.
5. PgBouncer를 실제 DB에 닿지 않는 경로로 시작하고 PAUSE합니다.
6. 저장 경로를 복원합니다. `HELD`는 차단을 유지합니다.
7. `OPEN`이면서 선택한 DB만 writer LOGIN인 경우에만 RESUME합니다.
8. backend 컨테이너에서 API와 Worker를 함께 실행합니다.

변경 의도와 `ROUTE`의 `HELD`를 같은 DynamoDB 트랜잭션으로 기록합니다. 파일 교체 전에 의도한 다음 경로를 저장합니다. 실제 경로·단독 writer·프록시 재개까지 확인한 RESUME만 `OPEN`을 저장합니다.

Registry가 바뀌었거나, 이전 기록은 있는데 경로가 없으면 시작을 거부합니다. 원본을 추측해서 선택하지 않습니다. 기존 PostgreSQL 상태의 자동 이관은 없습니다. 진행 중 작업의 상태 저장소를 빈 DynamoDB로 바꾸지 마십시오.

## 잠금과 장애

Worker 잠금에는 자동 만료나 TTL이 없습니다. 외부 변경 전에는 소유자를 확인하고, 상태 트랜잭션에도 소유자 조건을 넣습니다. 같은 프로세스가 일시 장애로 반납하지 못한 자기 잠금만 다시 확인할 수 있습니다. 새 프로세스가 이전 잠금을 시간 경과만으로 인계하지는 않습니다.

강제 종료 후에는 이전 ECS Task가 STOPPED이고 원격 DB·PgBouncer에 이전 명령이 남지 않았는지 확인합니다. 그다음 아래 명령을 실행합니다. CLI는 운영자가 제공한 근거를 기록하며 AWS의 종료 상태를 자동 조회하지 않습니다.

```sh
foxtrot-db locks
foxtrot-db recover-lock --lock-owner PREVIOUS_OWNER \
  --stopped-evidence 'ecs:OLD_TASK_ARN:STOPPED'
```

이후 RECONCILE과 최신 quiesceRef로 명시적인 후속 명령을 실행합니다.

| 상황 | 동작 |
|---|---|
| DynamoDB 일시 장애 | 새 변경을 멈춤. 정상 PgBouncer의 기존 업무 연결은 유지 |
| Task 시작 시 DynamoDB 장애 | 저장 경로를 확인할 수 없으므로 시작 거부 |
| Worker 강제 종료 | 잠금·실행 의도 보존. 이전 실행기 종료 확인 후 수동 복구 |
| 전환 중 Task 종료 | 새 Task는 HELD를 유지. 자동 원본 복귀 없음 |
| PgBouncer 종료 | ECS가 Task를 교체하며 앱 연결은 끊김 |
| Worker 코드 배포 | 같은 Task의 PgBouncer도 교체되므로 앱 재접속 필요 |

`/live`는 프로세스만, `/health`는 DynamoDB 연결도 검사합니다. ECS의 backend 검사는 `curl`로 loopback의 `/live`를 호출합니다. PgBouncer 검사는 `pooler.ready` 파일과 `pg_isready`를 사용합니다. 이 명령들은 Python·AWS SDK를 불러오지 않습니다. loopback HTTPS 상태 검사에서만 인증서 검증을 생략합니다. 앱·오케스트레이터의 실제 접속은 인증서를 검증해야 합니다. DynamoDB 장애만으로 정상 프록시를 재시작하지 않습니다.

`desiredCount=1`, `minimumHealthyPercent=0`, `maximumPercent=100`입니다. Task 교체 중 접속 중단이 있습니다. DNS 갱신은 기존 TCP 연결을 새 Task로 옮기지 않습니다.

## 검증 범위

로컬 검사는 DynamoDB Local, PostgreSQL 18, PgBouncer를 사용합니다. 실제 ECS·IAM·공인 네트워크 배포는 별도 검증이 필요합니다. DMS의 초기 복사·CDC 제어는 별도 CLI이며, 공통 API의 컷오버·롤백까지 연결하지 않았습니다.

참고: [DynamoDB 읽기 일관성](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html), [트랜잭션 IAM](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis-iam.html), [ECS 서비스 검색](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-discovery.html), [PgBouncer 설정](https://www.pgbouncer.org/config).
