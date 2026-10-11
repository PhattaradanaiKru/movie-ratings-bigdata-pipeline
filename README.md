# Movie Ratings Big Data Pipeline

โปรเจกต์วิเคราะห์คะแนนหนัง โดยใช้ MovieLens 32M เป็นข้อมูลคะแนนรายผู้ใช้ และ IMDb เป็นข้อมูลคะแนนสรุปอีกแหล่งหนึ่ง จุดเน้นคือการนำเข้าข้อมูล การตรวจคุณภาพ การแปลงด้วย Spark และการเตรียมตารางวิเคราะห์

## สิ่งที่โปรเจกต์ทำได้

- ดาวน์โหลด MovieLens 32M และ IMDb (`title.ratings`, `title.basics`) มาเก็บใน `data/raw/`
- มี Airflow DAG สำหรับตรวจไฟล์ แตกไฟล์ รัน PySpark โหลด PostgreSQL และตรวจผลลัพธ์
- งาน PySpark สร้างตาราง Parquet ใน `data/processed/`
- PostgreSQL เก็บข้อมูลที่ผ่าน cleansing ทั้งคะแนนรายคนและตารางสรุป
- มี PostgreSQL views สำหรับเชื่อม Power BI; HDFS, Hive และ Trino เป็นขั้นโครงสร้างพื้นฐานถัดไปหากวิชากำหนด
- ดึงข้อมูลเพิ่มจาก Wikipedia ด้วย web scraping และ Wikidata ผ่าน API เพื่อใช้วิเคราะห์รายได้และประเทศต้นทางของหนัง
- มี [ตัวอย่าง CSV จาก web scraping ของ Wikipedia](data/scraped/wikipedia_highest_grossing.csv) ใน Git พร้อม [แหล่งที่มาและเงื่อนไขการใช้](data/scraped/README.md)
- มี [CSV ผล Wikidata API](data/api/wikidata_films.csv) ใน Git พร้อม [รายละเอียด snapshot และเงื่อนไขการใช้](data/api/README.md)
- นำเข้าชุดข้อมูลผู้เข้าชิงและผู้ชนะ Oscar จากไฟล์ Oscars-Data ซึ่งผู้เผยแพร่รวบรวมจากเว็บ Academy

## รันครั้งแรกบนเครื่องใหม่

ต้องมี **Git และ Docker Desktop/Engine พร้อม Docker Compose** พร้อมอินเทอร์เน็ตสำหรับดาวน์โหลด dataset และ Docker images แนะนำให้มี RAM อย่างน้อย 8 GB และพื้นที่ว่างอย่างน้อย 10 GB เพราะฐานข้อมูลคะแนน 32 ล้านแถวใช้พื้นที่หลาย GB ตรวจให้พอร์ต `8080` และ `5432` บนเครื่องว่างก่อนเริ่ม

```bash
git clone https://github.com/PhattaradanaiKru/movie-ratings-bigdata-pipeline.git
cd movie-ratings-bigdata-pipeline

docker compose up --build -d
```

Compose เตรียมโฟลเดอร์ข้อมูล เปิด PostgreSQL และ Airflow จากนั้น Airflow สร้าง DAG run อัตโนมัติ **หนึ่งรอบต่อ Airflow metadata volume** งานใน DAG ดาวน์โหลด MovieLens/IMDb ลง `data/raw/`, ตรวจไฟล์, แปลงข้อมูล และโหลด PostgreSQL โดยไม่ต้องติดตั้ง Python หรือ Make บนเครื่องโฮสต์ คำสั่ง `up -d` กลับมาทันที แต่งานดาวน์โหลดและประมวลผลยังทำต่อเบื้องหลัง ให้รอ DAG ในหน้า Airflow ขึ้น **Success** ก่อนตรวจฐานข้อมูล การสั่ง `docker compose up -d` อีกครั้งโดยยังเก็บ volumes เดิมจะไม่สร้าง DAG run ใหม่

เปิด [Airflow](http://localhost:8080) ด้วย **username `airflow` / password `airflow`** DAG จะทำ `download_sources → verify_sources → extract_movielens → spark_transform → validate_output → load_postgres → validate_database → external_enrichment` ขยายกลุ่ม `external_enrichment` ในหน้า Graph จะเห็น `fetch_wikipedia_box_office`, `fetch_wikidata_movie_details` และ `fetch_oscars_awards` ทำงานแยกกัน ก่อน `publish_external_to_postgres` ตรวจ snapshot และโหลดข้อมูลลงฐานข้อมูล งานดึงข้อมูลต้องใช้อินเทอร์เน็ตในครั้งแรก Wikidata สอบถาม IMDb IDs ทั้ง 87,585 เรื่องแบบทยอยเรียก จึงอาจใช้เวลาหลายนาทีและรันต่อจาก snapshot ได้หากงานสะดุด หาก task ล้มเหลวหลัง retry ให้แก้สาเหตุแล้วกด Clear task ใน Airflow หรือสั่ง `make airflow-trigger` เพื่อรันใหม่ด้วยมือ

หลัง DAG สำเร็จ ตรวจจำนวนข้อมูลด้วย:

```bash
make db-counts
```

ผลของ MovieLens 32M ควรเป็น **32,000,204 คะแนน** และ **87,585 เรื่อง** ค่า IMDb อาจเปลี่ยนตามวันที่ดาวน์โหลด `make airflow-down` หยุดบริการโดยเก็บข้อมูล PostgreSQL ไว้ใน Docker volume การ trigger DAG ใหม่จะสร้างผลลัพธ์และสลับตาราง PostgreSQL ใหม่ ไม่เพิ่มแถวซ้ำ

## โครงสร้าง

```text
data/raw/                    ไฟล์ต้นทาง (ไม่เก็บใน Git)
data/processed/              Parquet ที่ Spark สร้าง (ไม่เก็บใน Git)
src/movie_pipeline/          ตัวตรวจไฟล์และงาน Spark
airflow/dags/                 Airflow DAG สำหรับควบคุมลำดับงาน
docs/                        แผนข้อมูลและการพัฒนาต่อ
skills/                      สกิลประจำโปรเจกต์สำหรับ Codex
```

## ข้อมูลและตาราง

task `verify_sources` ตรวจ MD5 ของ MovieLens ZIP, ตรวจว่า ZIP/GZip เปิดได้ และเช็กไฟล์ที่ต้องใช้ Spark อ่าน `ratings.csv`, `movies.csv` และ `links.csv` หลัง Airflow แตก ZIP ส่วนไฟล์ IMDb อ่านจาก GZip โดยตรง

ตาราง PostgreSQL ที่ DAG สร้างคือ `fact_ratings`, `dim_movies`, `movie_metrics` และ `genre_metrics` Spark cleansing ข้อมูลก่อนแล้วโหลดผ่านตาราง staging จากนั้นสลับเป็นตารางจริงเมื่อโหลดครบ

หลังโหลดเสร็จ DAG สร้าง views `powerbi_overview`, `powerbi_movies`, `powerbi_release_year` และ `powerbi_genres` สำหรับนำเข้า Power BI พร้อมตรวจว่ามีข้อมูล ดูขั้นตอนเชื่อมต่อ กราฟที่แนะนำ และข้อควรระวังเรื่องสเกลคะแนนใน [คู่มือ Power BI](docs/power-bi.md)

ข้อมูลภายนอกเพิ่มเติมอยู่ในตาราง `web_top_grossing_films` (Wikipedia HTML), `api_wikidata_films` (Wikidata SPARQL API) และ `oscars_awards` (ไฟล์ Oscars-Data) พร้อม views สำหรับรายได้ ประเทศต้นทาง และรางวัล ดูรายละเอียดการดึง การจับคู่ และข้อจำกัดใน [คู่มือแหล่งข้อมูลภายนอก](docs/external-sources.md) หากต้องการรันเฉพาะขั้นนี้หลัง DAG เคยสำเร็จแล้ว ใช้ `make external-refresh`

การดึง Wikidata ล่าสุดสอบถามครบ **87,585 IMDb IDs** พบ **76,350 เรื่อง (87.17%)** และไม่พบ **11,235 เรื่อง** View `powerbi_wikidata_movies` ยังคงแสดงหนังครบ 87,585 เรื่อง โดยค่า Wikidata เป็น `NULL` เมื่อไม่พบ

### เปิดข้อมูลด้วย DBeaver

เพิ่ม connection ประเภท **PostgreSQL** ใน DBeaver แล้วกรอก:

| ช่อง | ค่า |
| --- | --- |
| Host | `localhost` |
| Port | `5432` |
| Database | `movie_ratings` |
| Username | `movie` |
| Password | `movie` |

กด **Test Connection** แล้วเปิด schema `public` จะเห็นทั้ง 4 ตารางหลัง DAG สำเร็จ พอร์ต PostgreSQL ใน Compose ผูกเฉพาะ `127.0.0.1` เพื่อให้ต่อจากเครื่องนี้ได้ หาก Docker Desktop ปิดอยู่ให้เปิดก่อนแล้วรัน `make airflow-up`

ถ้าพอร์ต `5432` ถูกโปรแกรมอื่นใช้อยู่ ให้เปลี่ยนฝั่งซ้ายของ mapping ใน `compose.yaml` จาก `127.0.0.1:5432:5432` เป็น `127.0.0.1:5433:5432` แล้วใช้พอร์ต `5433` ใน DBeaver

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
