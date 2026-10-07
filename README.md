# Movie Ratings Big Data Pipeline

โปรเจกต์วิเคราะห์คะแนนหนัง โดยใช้ MovieLens 32M เป็นข้อมูลคะแนนรายผู้ใช้ และ IMDb เป็นข้อมูลคะแนนสรุปอีกแหล่งหนึ่ง จุดเน้นคือการนำเข้าข้อมูล การตรวจคุณภาพ การแปลงด้วย Spark และการเตรียมตารางวิเคราะห์

## สถานะปัจจุบัน

- ดาวน์โหลด MovieLens 32M และ IMDb (`title.ratings`, `title.basics`) ไว้ใน `data/raw/` แล้ว
- มี Airflow DAG สำหรับตรวจไฟล์ แตกไฟล์ รัน PySpark โหลด PostgreSQL และตรวจผลลัพธ์
- งาน PySpark สร้างตาราง Parquet ใน `data/processed/`
- PostgreSQL เก็บข้อมูลที่ผ่าน cleansing ทั้งคะแนนรายคนและตารางสรุป
- HDFS, Hive, Trino และ dashboard ยังเป็นขั้นต่อไป

## โครงสร้าง

```text
data/raw/                    ไฟล์ต้นทาง (ไม่เก็บใน Git)
data/processed/              Parquet ที่ Spark สร้าง (ไม่เก็บใน Git)
src/movie_pipeline/          ตัวตรวจไฟล์และงาน Spark
airflow/dags/                 Airflow DAG สำหรับควบคุมลำดับงาน
docs/                        แผนข้อมูลและการพัฒนาต่อ
skills/                      สกิลประจำโปรเจกต์สำหรับ Codex
```

## เตรียมข้อมูล

ข้อมูลถูกดาวน์โหลดไว้แล้ว หากเริ่มบนเครื่องอื่นให้ใช้:

```bash
make download
make verify
```

`make verify` ตรวจ MD5 ของ MovieLens ZIP, ตรวจว่า ZIP/GZip เปิดได้ และเช็กไฟล์ที่ต้องใช้ ไม่ต้องแตก ZIP ด้วยมือ งาน Spark อ่าน `ratings.csv`, `movies.csv` และ `links.csv` จาก ZIP หลังคำสั่ง `make extract` ส่วนไฟล์ IMDb อ่านจาก GZip โดยตรง

## รัน pipeline ผ่าน Airflow

ต้องเปิด Docker Desktop ก่อน แล้วรัน:

```bash
make airflow-up
make airflow-trigger
```

เปิด Airflow ที่ `http://localhost:8080` โดยใช้ **username `airflow` และ password `airflow`** แล้วดู DAG ชื่อ `movie_ratings_pipeline` บัญชีนี้ตั้งไว้สำหรับการพัฒนาในเครื่องเท่านั้น และหน้าเว็บเปิดเฉพาะ localhost

DAG ทำงานตามลำดับ `verify_sources → extract_movielens → spark_transform → validate_output → load_postgres → validate_database` โดย Spark cleansing ข้อมูลก่อน แล้วโหลดข้อมูลลง PostgreSQL ผ่านตาราง staging จากนั้นสลับเป็นตารางจริงเมื่อโหลดครบ ตั้งให้เริ่มด้วยการ trigger เอง ไม่มีการดาวน์โหลด snapshot เดิมทุกวัน `make airflow-down` หยุด Airflow และ PostgreSQL โดยไม่ลบข้อมูลใน volume

ตรวจจำนวนข้อมูลในฐานข้อมูลหลัง DAG สำเร็จด้วย `make db-counts` ตารางที่สร้างคือ `fact_ratings`, `dim_movies`, `movie_metrics` และ `genre_metrics`

### เปิดข้อมูลด้วย DBeaver

เพิ่ม connection ประเภท **PostgreSQL** ใน DBeaver แล้วกรอก:

| ช่อง | ค่า |
| --- | --- |
| Host | `localhost` |
| Port | `5432` |
| Database | `movie_ratings` |
| Username | `movie` |
| Password | `movie` |

กด **Test Connection** แล้วเปิด schema `public` จะเห็นทั้ง 4 ตาราง พอร์ต PostgreSQL ใน Compose ผูกเฉพาะ `127.0.0.1` เพื่อให้ต่อจากเครื่องนี้ได้ หาก Docker Desktop ปิดอยู่ให้รัน `make airflow-up` ก่อน

## รันเฉพาะ Spark โดยตรง

ใช้ [Docker Compose](compose.yaml) กับ Spark 3.5.3 เพื่อทดสอบขั้นแปลงข้อมูลแยกจาก Airflow:

```bash
make extract
make spark-transform
```

หรือรันตรงด้วย `docker compose run --rm spark-transform` โดย Compose จะ mount โฟลเดอร์โปรเจกต์ที่ `/workspace` และเขียน Parquet กลับมาที่ `data/processed/` คำสั่งนี้ใช้ทดสอบ Spark แยก และ **ไม่ได้โหลด PostgreSQL** หากต้องการรัน pipeline ครบให้ trigger DAG ผ่าน Airflow

หากรันบนเครื่องโดยตรง ต้องมี Python 3.10–3.12, Java 11 หรือ 17 และ PySpark 3.5:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
make transform
```

กำหนดทรัพยากรผ่าน `SPARK_MASTER`, `SPARK_DRIVER_MEMORY`, `SPARK_SHUFFLE_PARTITIONS` ได้ ค่าเริ่มต้นอยู่ใน `src/movie_pipeline/transform.py` การรัน `make transform` เขียนผลใหม่ที่ `data/processed/` เพื่อให้รันซ้ำแล้วไม่เพิ่มข้อมูลซ้ำ

ผลลัพธ์หลัก:

| ตาราง | ความหมาย |
| --- | --- |
| `fact_ratings` | คะแนนรายผู้ใช้ MovieLens พร้อมปีที่ให้คะแนน |
| `dim_movies` | หนังและรหัสเชื่อม IMDb พร้อมข้อมูลประเภท/ปีที่ฉาย |
| `movie_metrics` | จำนวนคะแนนและค่าเฉลี่ย MovieLens เปรียบเทียบกับ IMDb |
| `genre_metrics` | คะแนนตามประเภทหนัง |
| `quality_report.json` | จำนวนแถวและจำนวนรายการที่ join ไม่ได้ |

## หลักการวิเคราะห์

MovieLens เป็นคะแนนรายผู้ใช้แบบ 0.5–5 ดาว ขณะที่ IMDb `averageRating` เป็นคะแนนสรุประดับเรื่องแบบ 1–10 อย่านำมาบวกหรือเฉลี่ยกันตรง ๆ ให้แสดงแยกแหล่ง หรือแปลงสเกลอย่างชัดเจนเมื่อเปรียบเทียบ และบันทึกวันที่ดาวน์โหลด IMDb เพราะข้อมูลนี้เปลี่ยนได้

ชุด MovieLens 32M เป็น snapshot คงที่ จึงไม่ควรตั้งให้ดาวน์โหลดซ้ำทุกวัน หากเพิ่ม Airflow ภายหลังให้ DAG ตรวจว่ารุ่นนี้ถูกนำเข้าแล้ว และให้รัน transformation ซ้ำได้อย่างปลอดภัย

## แหล่งข้อมูล

- [MovieLens 32M และเงื่อนไขการใช้](https://grouplens.org/datasets/movielens/32m/)
- [IMDb non-commercial datasets](https://www.imdb.com/interfaces/)

ตรวจเงื่อนไขการใช้และใส่แหล่งอ้างอิงในรายงานก่อนเผยแพร่ผลลัพธ์
