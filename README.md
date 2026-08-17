# GoFlow CQRS

GoFlow คือโปรเจกต์ Go backend เพื่อเรียนรู้การออกแบบระบบจริงแบบค่อยเป็นค่อยไป โดยเริ่มจาก **modular monolith** ที่แยก Command และ Query ออกจากกันอย่างชัดเจน แต่ยังมีขนาดเล็กพอที่จะอ่านและอธิบายได้ทั้งระบบ

## เป้าหมายของโปรเจกต์

- ฝึก Go แบบ idiomatic ผ่านปัญหา backend จริง
- ใช้ Clean Architecture และ CQRS โดยมี write model กับ read model แยกกัน
- ใช้ PostgreSQL, GORM, `database/sql`, transaction และ optimistic concurrency อย่างมีเหตุผล
- เรียนรู้ Domain Events, Transactional Outbox, goroutine, channel และ worker pool
- มี Docker Compose, Swagger UI, tests และ README ที่ใช้เป็น portfolio ได้

## สิ่งที่ทำได้แล้ว

- Gin HTTP API สำหรับ Command และ Query
- write model แบบ normalized ใน PostgreSQL โดยใช้ GORM
- read model ใน schema `goflow_read` โดย query ด้วย `database/sql`
- Job state machine และ `JobAttempt` lifecycle ใน domain layer
- Transactional Outbox: ทุก Command เขียน state และ event ภายใน transaction เดียวกัน
- read-model projector แบบ asynchronous, idempotent และใช้ `FOR UPDATE SKIP LOCKED` เพื่อ claim event
- bounded channel และ projector workers สำหรับประมวลผล event
- Docker Compose สำหรับ PostgreSQL, API และ Swagger UI

## ภาพรวมการทำงาน

```text
Command HTTP API
  → Command Service
  → Domain Aggregate
  → PostgreSQL write tables + outbox_events (transaction เดียว)
  → Projector worker / channel
  → goflow_read read models
  → Query HTTP API
```

Command response มาจาก write side โดยตรง ส่วน `GET` อ่าน read model ที่ projector อัปเดตแบบ asynchronous ดังนั้นหลัง `POST` สำเร็จ การ `GET` ทันทีอาจยังพบ `404` ชั่วครู่ได้ นี่คือ eventual consistency ที่ตั้งใจออกแบบไว้

## โครงสร้างหลัก

```text
cmd/api/                         จุดเริ่มต้นของ HTTP server และ graceful shutdown
internal/domain/                 state machine, aggregate และ domain errors
internal/app/command/            command orchestration และ input validation
internal/adapter/postgres/write/ GORM write persistence และ transactional outbox
internal/adapter/postgres/read/  database/sql read queries
internal/projection/             outbox poller, channel และ projector workers
internal/adapter/httpapi/        Gin routes และ HTTP error mapping
migrations/                      PostgreSQL schema
api/openapi.yaml                 OpenAPI document สำหรับ Swagger UI
```

## รันด้วย Docker

```bash
copy .env.example .env
docker compose up --build
```

- API: <http://localhost:8080>
- Health check: <http://localhost:8080/healthz>
- Swagger UI: <http://localhost:8081>

PostgreSQL จะ apply SQL ใน `migrations/` เมื่อสร้าง volume ใหม่ หากต้องการล้างฐานข้อมูลสำหรับ development โดยตั้งใจ:

```bash
docker compose down -v
docker compose up --build
```

> คำสั่งนี้ลบ Docker volume ของ GoFlow และข้อมูล local ทั้งหมด

## ทดลอง API ผ่าน Swagger UI

เปิด <http://localhost:8081> แล้วเรียกตามลำดับนี้

1. `POST /v1/projects`

   ```json
   { "name": "Demo Project" }
   ```

2. `POST /v1/projects/{projectID}/tasks`

   ```json
   {
     "name": "echo",
     "kind": "demo",
     "maxAttempts": 2,
     "timeoutMs": 30000
   }
   ```

3. `POST /v1/tasks/{taskID}/jobs`

   ```json
   {
     "idempotencyKey": "demo-1",
     "input": { "message": "hello" }
   }
   ```

4. รอ projector ชั่วครู่ แล้วเรียก `GET /v1/jobs/{jobID}`
5. `POST /v1/jobs/{jobID}/start` พร้อม `workerId`
6. นำ `attempt.id` ที่ได้ไปเรียก `/complete` หรือ `/fail`

`/start`, `/complete` และ `/fail` เป็น demo actions เพื่อให้ฝึก Job lifecycle และเห็นผลของ worker flow ก่อนจะเพิ่ม real task executor ใน milestone ถัดไป

## Job state machine

```text
QUEUED → RUNNING → SUCCESS
                 ↘ FAILED → RETRYING → RUNNING

QUEUED / RETRYING / FAILED → CANCELLED
```

การ cancel Job ที่กำลัง `RUNNING` ยังไม่อยู่ใน initial version เพราะต้องออกแบบ cooperative cancellation ระหว่าง executor, context และ state transition ให้ถูกต้องก่อน

## คำศัพท์สำคัญ

- **CQRS (Command Query Responsibility Segregation)** — แยกเส้นทางที่เปลี่ยนข้อมูล (`Command`) ออกจากเส้นทางที่อ่านข้อมูล (`Query`) เพื่อให้แต่ละด้านมี model ที่เหมาะกับหน้าที่ของตน
- **Command** — คำสั่งที่อาจเปลี่ยน state ของระบบ เช่น สร้าง Project, ส่ง Job หรือ complete Job
- **Query** — คำขออ่านข้อมูลที่ไม่เปลี่ยน state เช่น ดู Job, history หรือ dashboard
- **write model** — ตารางข้อมูลหลักที่เก็บ state แบบ normalized และใช้ตัดสิน business rules
- **read model** — ตารางที่จัดรูปข้อมูลไว้เพื่ออ่านเร็วและตรงกับหน้าจอ/API อาจ denormalized ได้
- **Domain Aggregate** — กลุ่มของ entity ที่ต้องรักษา business rules ร่วมกันภายใน transaction เดียวกัน; ในโปรเจกต์นี้ `Job` เป็น aggregate ที่ดูแล state และ `JobAttempt`
- **Domain Event** — ข้อเท็จจริงที่เกิดขึ้นใน domain เช่น `JobStarted` หรือ `JobCompleted` ซึ่งบอกสิ่งที่เกิดแล้ว ไม่ใช่คำสั่งให้ทำงาน
- **Transactional Outbox** — รูปแบบที่บันทึก state ของ domain และ event ลงฐานข้อมูลใน transaction เดียวกัน เพื่อไม่ให้เกิดกรณีข้อมูลเปลี่ยนแต่ event สูญหาย
- **projector** — worker ที่อ่าน event จาก `outbox_events` แล้วอัปเดต `goflow_read` ให้กลายเป็น read model
- **eventual consistency** — write model อัปเดตสำเร็จก่อน แล้ว read model จะตามมาในเวลาสั้น ๆ ดังนั้น `GET` ทันทีหลัง `POST` อาจยังไม่เห็นข้อมูล
- **idempotency** — การทำ event เดิมซ้ำแล้วได้ผลลัพธ์สุดท้ายเหมือนเดิม; `projection_receipts` ใช้ป้องกัน projector นับหรือเขียนข้อมูลซ้ำ
- **optimistic concurrency** — ป้องกัน writer หลายรายแก้ Job เดียวกันพร้อมกันด้วย `version` โดย update จะสำเร็จเมื่อ version ยังตรงกับที่อ่านมา
- **goroutine** — lightweight concurrent function ของ Go ที่ใช้รัน poller หรือ projector worker
- **channel** — กลไกส่งข้อมูลระหว่าง goroutine; โปรเจกต์ใช้ bounded channel เพื่อทำ backpressure ระหว่าง poller กับ worker
- **worker pool** — กลุ่ม worker หลายตัวที่รับงานจาก channel เพื่อเพิ่ม throughput โดยจำกัดจำนวนงาน concurrent
- **GORM** — ORM ที่ใช้กับ write side และ transaction ของ Command
- **`database/sql`** — standard library package ที่ใช้ query read model ด้วย SQL แบบ explicit
- **OpenAPI / Swagger UI** — OpenAPI เป็นเอกสารสัญญาของ HTTP API; Swagger UI คือหน้าเว็บสำหรับอ่านและทดลอง call API จาก browser

## ดู state ของ Job และ worker ในฐานข้อมูล

หลังทดลอง API สามารถดูผลลัพธ์ได้จากตารางต่อไปนี้:

- `projects`, `tasks`, `jobs`, `job_attempts` — write model
- `outbox_events` — Domain Events ที่รอ/ถูก projector ประมวลผล
- `goflow_read.project_views`, `task_views`, `job_views` — read model
- `goflow_read.project_dashboard_views` — dashboard counters
- `goflow_read.projection_receipts` — idempotency record ของ projector

ตัวอย่าง:

```bash
docker compose exec postgres psql -U goflow -d goflow
```

จากนั้นทดลอง:

```sql
SELECT sequence, event_type, aggregate_id, processed_at, attempt_count
FROM outbox_events
ORDER BY sequence;

SELECT job_id, status, attempt_count, last_error
FROM goflow_read.job_views;
```

## ตรวจสอบคุณภาพ

```bash
go test ./...
go vet ./...
go test -race ./...
```

`go test -race` ต้องใช้ CGO และ C compiler เช่น `gcc` บน Windows หากยังไม่มี compiler คำสั่งนี้จะรันไม่ได้ แม้ `go test` ปกติจะผ่าน

## ขอบเขตที่ยังตั้งใจไม่ทำ

- Kafka, Redis, microservices, Kubernetes และ event sourcing
- real task executor และ cooperative cancellation ของ running Job
- keyset cursor pagination, dead-letter operations และ integration test suite แบบเต็ม

สิ่งเหล่านี้เป็น milestone ถัดไป เพื่อให้การเรียนรู้เน้นความถูกต้องและความเข้าใจก่อนเพิ่ม infrastructure

## ขั้นต่อไป

1. นิยาม Task kinds และสร้าง real executor worker pool
2. เพิ่ม PostgreSQL integration tests สำหรับ rollback, duplicate event, ordering และ shutdown
3. เพิ่ม keyset pagination ใน `ListJobs`
4. เขียน ADR สำหรับ CQRS, aggregate boundaries และ Transactional Outbox
