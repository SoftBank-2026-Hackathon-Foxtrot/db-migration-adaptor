# AWS RDS → 온프렘 PostgreSQL 17

온프렘을 대상으로 쓸 때만 `POST /v1/migrations`에
`profile: postgres-rds-onprem-logical-pgbouncer-v1`을 명시한다. AWS DMS 작업이나
Cloud SQL은 필요하지 않다. 기존 AWS ↔ GCP 프로필과 기본값은 그대로다.

1. 원본은 독립 AWS RDS PostgreSQL 17, 대상은 독립 PostgreSQL 17 primary다. 양쪽에
   `wal_level=logical`이 필요하다. 어댑터와 PgBouncer에서 두 DB에 접근할 수 있어야 하고,
   온프렘 PostgreSQL 컨테이너에서도 원본 RDS 주소로 복제 접속이 가능해야 한다.
2. 원본과 같은 업무 테이블·PK·시퀀스를 대상에 **데이터 없이** 준비한다. writer 이름과
   (동적 binding의 경우) 비밀번호를 원본에 맞추되 대상은 `NOLOGIN`으로 둔다.
   온프렘 관리 계정에는 실제 superuser 권한이 필요하다. 전용 앱·binding·워크로드를 사용한다.
   현재 온프렘 생성기의 기본 writer는 `app_writer`이지만 원본이 `app`이면 대상도
   `app`으로 준비한다. 스키마 생성 계정(`app_schema`)이 테이블 소유자라면 bootstrap을
   마친 뒤 대상에서 `NOLOGIN`으로 두고 세션을 닫는다. `NOLOGIN`만으로 기존 연결은
   종료되지 않는다. PREPARE 시 대상에 다른 클라이언트 연결이나 prepared transaction이 없어야 한다.
   운영 중인 클라우드 경로와 같은 원본 DB/writer를 사용하면 실제 전환은 해당 원본의 쓰기를
   차단한다. 병행 리허설은 반드시 별도 원본과 PgBouncer binding으로 한다.
3. 온프렘 대상의 전용 Secret은 `adminDsn`, `replicationDsn`, `writerPassword`를 담는다.
   대상 DSN 두 개 모두 `sslmode=verify-full`로 하고 인증서 SAN이 실제 접속 주소에 맞아야
   한다. CA는 어댑터가 읽을 수 있어야 한다. 원본 replication DSN에 CA/키 경로를 지정하는
   경우 어댑터와 실제 구독자 컨테이너 **양쪽**에서 그 경로를 읽을 수 있어야 한다.
   API에는 비밀 원문 대신 `secretRef` ARN만 보낸다.
4. 어댑터의 `GET /v1/capabilities`에서 프로필을 확인하고 현재 AWS binding으로 다음 요청을 보낸다.
   자리표시자는 실제 값으로 교체한다. 기존 binding은 재등록할 필요가 없다.

```json
{
  "workflowId": "<onprem-workflow-id>",
  "resourceId": "database",
  "kind": "DATABASE",
  "profile": "postgres-rds-onprem-logical-pgbouncer-v1",
  "source": {
    "provider": "AWS_RDS", "providerInstance": "<RDS ARN>",
    "host": "<RDS DNS>", "port": 5432, "database": "app",
    "writerRole": "app", "secretRef": "<source operator secret ARN>"
  },
  "target": {
    "provider": "SELF_MANAGED", "host": "<VM address matching the certificate>",
    "port": 5432, "database": "appdb", "writerRole": "app",
    "secretRef": "<target operator secret ARN>"
  },
  "policy": {"rollbackMode": "BEFORE_TARGET_WRITE", "maxQuiesceSeconds": 60}
}
```

`PREPARE`은 기존 데이터를 지우지 않는다. 스키마, 역할, 대상 데이터, TLS, 등록한
DB와 실제 연결이 다르면 진행을 중단한다. `START_SYNC`는 원본 publication과 온프렘
subscription을 만들어 초기 복사와 후속 변경을 복제한다. `READY`는 전환 가능 상태일
뿐이다. 실제 업무 HOLD 근거가 있어야 `QUIESCE → CUTOVER → RESUME`으로 진행하며,
원본 writer가 차단되고 마지막 변경이 대상에 적용됐다는 근거를 확인하고 PgBouncer를
전환한다. 대상 쓰기 허용 이후의 자동 원복이나 온프렘 → AWS 역복제는 제공하지 않는다.
