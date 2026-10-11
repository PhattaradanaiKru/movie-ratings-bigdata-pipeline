# แหล่งข้อมูลภายนอก: web scraping, API และ Oscars

Airflow ขั้น `enrich_external` เพิ่มแหล่งข้อมูลหลัง PostgreSQL หลักผ่านการตรวจแล้ว:

| ประเภท | แหล่ง | วิธี | ผลลัพธ์ |
| --- | --- | --- | --- |
| Web scraping | [Wikipedia: List of highest-grossing films](https://en.wikipedia.org/wiki/List_of_highest-grossing_films) | อ่านตาราง HTML “Highest-grossing films” | `web_top_grossing_films`, `powerbi_box_office` |
| API | [Wikidata Query Service](https://query.wikidata.org/) | SPARQL ค้น IMDb ID (P345) ของหนัง MovieLens ทุกเรื่องที่มีรหัส | `api_wikidata_films`, `powerbi_wikidata_movies` |
| Oscars dataset | [Oscars-Data โดย Maura](https://github.com/mauraanish/Oscars-Data) | ดาวน์โหลด CSV ที่ตรึง commit และตรวจ SHA-256 | `oscars_awards`, `powerbi_oscars_movies`, `powerbi_oscars_awards` |

ข้อมูลที่ดึงเก็บเป็น snapshot ใน `data/raw/external/` ซึ่งไม่ขึ้น Git และรันซ้ำโดยอ่าน snapshot เดิม ตัวอย่างผล web scraping ที่นำขึ้น Git คือ [`data/scraped/wikipedia_highest_grossing.csv`](../data/scraped/wikipedia_highest_grossing.csv) จำนวน 49 แถว พร้อมรายละเอียดแหล่งที่มาและสัญญาอนุญาตใน [`data/scraped/README.md`](../data/scraped/README.md) ไฟล์ตัวอย่างนี้ไม่ได้ใช้แทน snapshot ใน pipeline การเปลี่ยนชุดข้อมูลที่เลือกจาก MovieLens จะทำให้ชื่อไฟล์ API snapshot เปลี่ยนตาม hash ของ IMDb IDs หากต้องการดึงข้อมูลสดจาก Wikipedia/Wikidata ให้ย้ายหรือลบเฉพาะ snapshot สองแหล่งนั้นแล้วรัน `make external-refresh` ตารางปลายทางเขียนใหม่ภายใน transaction จึงไม่เพิ่มแถวซ้ำ

Wikidata ใช้ batch ละไม่เกิน **200 IMDb IDs** เรียกแบบลำดับเดียว เว้นอย่างน้อยหนึ่งวินาทีหลังคำขอใหม่ และเมื่อได้รับ HTTP 429/ข้อผิดพลาดชั่วคราวจะเคารพ `Retry-After` หรือพักแบบเพิ่มเวลา หากชุดใหญ่หมดเวลาจะลดขนาดชุดลง [Wikidata Query Service จำกัดเวลา query และโควตาการประมวลผลต่อผู้ใช้](https://www.mediawiki.org/wiki/Wikidata_query_service/Implementation) หากงานหยุดกลางทาง รัน `make external-refresh` ซ้ำได้โดยอ่าน batch ที่สำเร็จแล้วจาก snapshot คำว่า “ค้นครบ” หมายถึงส่ง IMDb ID ครบทุกเรื่อง ไม่ได้หมายความว่า Wikidata มีรายการตรงทุก ID ตาราง `api_wikidata_films` เก็บรายการที่พบ ส่วน `powerbi_wikidata_movies` เก็บหนัง MovieLens ทุกเรื่องและแสดงค่า Wikidata เป็น `NULL` เมื่อไม่พบ

ผล snapshot ล่าสุด: ค้น **87,585 IMDb IDs** ใน 438 ชุด พบ **76,350 IMDb IDs (87.17%)** ไม่พบ **11,235 IDs** ตาราง API มี 76,388 แถวเพราะ IMDb ID บางรายการมี Wikidata item มากกว่าหนึ่งรายการ View รวมผลต่อ IMDb ID ให้เหลือหนึ่งแถวต่อหนัง และมีประเทศต้นทางใน 75,921 เรื่อง

ข้อมูล Oscars มี **10,695 รายการ** สำหรับพิธีปี **1929–2024** ผู้เผยแพร่ [ระบุว่า scrape จากหน้า Oscars.org และปรับข้อมูลบางส่วนด้วยมือ](https://github.com/mauraanish/Oscars-Data#readme) ไฟล์นี้เผยแพร่พร้อม [MIT License](https://github.com/mauraanish/Oscars-Data/blob/main/LICENSE) เราตรึงไฟล์กับ commit `3b22c2c4bfb9fd6793043ee41d2a14f50ffd0201` และตรวจ SHA-256 ก่อนโหลด หน้า Oscars.org ปฏิเสธคำขออัตโนมัติด้วย HTTP 403 ในสภาพแวดล้อมที่ใช้ จึง **ไม่ได้ scrape เว็บ Oscars.org โดยตรง** ใน pipeline นี้ และข้อมูลยังไม่ครอบคลุมพิธีปี 2025–2026

ตาราง `oscars_awards` เก็บทุก category และสถานะชนะ/เข้าชิง View `powerbi_oscars_movies` สรุปจำนวนเข้าชิง/ชนะต่อหนังต่อปีพิธีและจับคู่กับ MovieLens เฉพาะปีพิธีตั้งแต่ 1980 โดยกำหนดปีฉาย = ปีพิธี − 1 และชื่อ normalize ตรงกันแบบไม่กำกวม รายการที่จับคู่ไม่ได้ยังอยู่โดย `movie_id` เป็น `NULL` จำนวนที่จับคู่ได้จาก snapshot ปัจจุบันคือ 1,187 คู่หนัง/ปี ตัวเลขนี้ไม่ควรตีความว่า Oscar มีหนังเพียงเท่านั้น เพราะ MovieLens ครอบคลุมไม่ครบและชื่ออาจต่างกัน

API จับคู่ด้วย IMDb ID ที่มีใน `movie_metrics.tconst` โดยตรง ส่วน Wikipedia ไม่มีรหัสหนัง จึงจับคู่เฉพาะชื่อที่ normalize แล้วตรงกัน **พร้อมปีที่ฉายตรงกันและพบ MovieLens เพียงเรื่องเดียว** รายการที่ไม่เข้าเงื่อนไขยังคงอยู่ใน `powerbi_box_office` แต่ `movie_id` เป็น `NULL`; อย่าอ้างว่าเป็นข้อมูลของเรื่องเดียวกันโดยอัตโนมัติ

ตรวจข้อมูลและอัตราการจับคู่:

```sql
SELECT count(*) AS scraped, count(movie_id) AS matched
FROM public.powerbi_box_office;

SELECT count(*) AS movie_rows,
       count(*) FILTER (WHERE wikidata_id IS NOT NULL) AS matched_movies,
       count(*) FILTER (WHERE wikidata_id IS NULL) AS unmatched_movies
FROM public.powerbi_wikidata_movies;

SELECT * FROM public.powerbi_box_office ORDER BY rank LIMIT 10;

SELECT count(*) AS film_years, count(movie_id) AS matched
FROM public.powerbi_oscars_movies;

SELECT ceremony_year, film_title, nomination_count, win_count,
       movielens_rating_count, movielens_average_rating
FROM public.powerbi_oscars_movies
WHERE movie_id IS NOT NULL
ORDER BY ceremony_year DESC, win_count DESC LIMIT 20;
```

รายงานจำนวนแถวอยู่ใน `data/processed/external_quality_report.json` รายได้จาก Wikipedia เป็นยอดรวมทั่วโลกตามรายการที่หน้าเว็บระบุ ซึ่งอาจรวมรอบฉายซ้ำ และไม่ใช่รายได้ปรับเงินเฟ้อ ข้อมูล Wikidata และหน้า Wikipedia เปลี่ยนได้ ให้ระบุวัน snapshot ในรายงาน [Wikimedia ขอให้ระบุ User-Agent และเคารพข้อจำกัดการเรียก API](https://www.mediawiki.org/wiki/Wikimedia_APIs/Access_policy); ตั้งค่า `EXTERNAL_SOURCE_USER_AGENT` เป็นชื่อโปรเจกต์พร้อมข้อมูลติดต่อเมื่อนำไปใช้งานต่อ
