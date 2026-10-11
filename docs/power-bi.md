# วิเคราะห์ข้อมูลด้วย Power BI

หลัง `make airflow-up` และ DAG `movie_ratings_pipeline` สำเร็จ ให้เชื่อม Power BI Desktop (บน Windows) กับ PostgreSQL ที่ `localhost:5432` ฐานข้อมูล `movie_ratings` ผู้ใช้ `movie` รหัสผ่าน `movie` เลือก **Import** แล้วเลือก views ใน schema `public` ดังนี้:

| View | ระดับข้อมูล | ใช้ทำอะไร |
| --- | --- | --- |
| `powerbi_overview` | ทั้งชุดข้อมูลหนึ่งแถว | การ์ดจำนวนหนัง จำนวนคะแนน MovieLens และจำนวน IMDb votes |
| `powerbi_movies` | หนังหนึ่งเรื่องต่อแถว | ตารางหนังยอดนิยม, scatter plot เปรียบเทียบ MovieLens กับ IMDb |
| `powerbi_release_year` | ปีที่หนังฉายหนึ่งปีต่อแถว | แนวโน้มจำนวนรีวิวและคะแนนตามปีที่หนังฉาย |
| `powerbi_genres` | แนวหนังและปีที่ฉายหนึ่งคู่ต่อแถว | คะแนนและจำนวนรีวิวตามแนวหนัง |
| `powerbi_box_office` | หนังรายได้สูงสุดใน Wikipedia หนึ่งเรื่องต่อแถว | เทียบรายได้กับคะแนนเมื่อ `movie_id` ไม่เป็น NULL |
| `powerbi_wikidata_movies` | หนัง MovieLens ทุกเรื่อง พร้อมข้อมูล Wikidata เมื่อพบ | วิเคราะห์ประเทศต้นทางกับคะแนนและดูความครอบคลุม |
| `powerbi_wikidata_countries` | หนังหนึ่งเรื่องต่อประเทศต้นทางหนึ่งประเทศ | นับหนังและคะแนนตามประเทศ หนังร่วมทุนถูกนับในทุกประเทศที่ร่วมสร้าง |
| `powerbi_oscars_movies` | หนังหนึ่งเรื่องต่อปีพิธี Oscar | เปรียบเทียบจำนวนรางวัลกับคะแนนหนัง |
| `powerbi_oscars_awards` | รางวัลหนึ่งรายการต่อแถว | กรองหมวดรางวัลและสถานะชนะ/เข้าชิง |

## เปิด dashboard สำเร็จรูป

โฟลเดอร์ `powerbi/` มี Power BI Project ที่เชื่อม views ข้างต้น สร้าง DAX measures และหน้า dashboard ไว้แล้ว เปิด `powerbi/MovieRatings.pbip` ด้วย Power BI Desktop แล้วกด **Refresh** เมื่อถามสิทธิ์ให้เลือกแท็บ **Database** ใส่ `movie` / `movie` และถ้าถามเรื่อง encryption ให้กด OK เพื่อต่อแบบไม่เข้ารหัส ถ้าใช้พอร์ตอื่น แก้ parameter `PgServer` ใน **Transform data → Manage parameters** ไฟล์ cache ข้อมูล (`.pbi/cache.abf`) ถูก ignore ใน Git

หน้าใน dashboard คือ ภาพรวม, หนังรายเรื่อง (กรองหนังที่มีรีวิว ≥ 1,000 ทั้งสองแหล่ง ปรับได้ในแผง Filters), แนวหนัง, รายได้กับคะแนน, ประเทศต้นทาง และ Oscar ทุกหน้าใช้สีน้ำเงินแทน MovieLens และสีส้มแทน IMDb

views สี่ชุดแรกสร้างในขั้น `load_postgres` และ views ข้อมูลภายนอกสร้างในขั้น `publish_external_to_postgres` จึงไม่ต้องรัน SQL ด้วยมือ Power BI Desktop รุ่นปัจจุบันมี PostgreSQL connector และ Npgsql มาให้แล้ว ใช้ **Refresh** หลัง DAG รอบใหม่สำเร็จเพื่อดึง snapshot ล่าสุด หากเชื่อมจากคนละเครื่อง `localhost` จะหมายถึงเครื่องที่รัน Power BI ต้องตั้งค่าการเข้าถึง PostgreSQL ของเครื่องที่รัน Docker ก่อน

## หน้า dashboard ที่แนะนำ

1. **ภาพรวม**: card จำนวนคะแนน MovieLens, จำนวนหนังที่มีรีวิว และจำนวน IMDb votes จาก `powerbi_overview` พร้อมกราฟจำนวนคะแนนตามปีที่หนังฉายจาก `powerbi_release_year` บางเรื่องไม่มีปีที่ฉาย จึงไม่ควรใช้ผลรวมของกราฟปีเป็นยอดรวมทั้งชุด
2. **หนังรายเรื่อง**: ตาราง `title`, `release_year`, `movielens_rating_count`, `movielens_average_rating`, `imdb_vote_count`, `imdb_average_rating` ใช้ตัวกรองจำนวนรีวิวขั้นต่ำ เช่น MovieLens ≥ 1,000 และ IMDb ≥ 1,000 เพื่อไม่ให้หนังที่มีคะแนนเพียงไม่กี่ครั้งครองอันดับ
3. **แนวหนัง**: bar chart คะแนนเฉลี่ยตาม `genre` และตัวกรองปีที่ฉาย; ถ้ารวมหลายปีให้ใช้ DAX measure แบบถ่วงน้ำหนักด้านล่าง

สร้าง measure ในตาราง `powerbi_genres`:

```dax
MovieLens ratings = SUM(powerbi_genres[rating_count])

MovieLens average =
DIVIDE(
    SUMX(powerbi_genres, powerbi_genres[rating_count] * powerbi_genres[movielens_average_rating]),
    SUM(powerbi_genres[rating_count])
)
```

สำหรับตารางรายเรื่อง ให้กรองหนังที่มีจำนวนรีวิวตามเกณฑ์ก่อนเรียงคะแนน การนำ `movielens_average_rating` ไป **Average** ข้ามหลายเรื่องจะให้น้ำหนักทุกเรื่องเท่ากัน จึงไม่เท่ากับค่าเฉลี่ยของรีวิวทั้งหมด ใช้ view รายปีซึ่งคำนวณค่าเฉลี่ยแบบถ่วงจำนวนรีวิวแล้วสำหรับกราฟตามปี

**ข้อควรระวังในการตีความ:** MovieLens ใช้ 0.5–5 ดาว ส่วน IMDb ใช้ 1–10 คะแนนและเป็นคะแนนสรุปจากผู้ใช้คนละกลุ่ม ให้แสดงสองค่าแยกกัน หากเปรียบเทียบเชิงภาพให้ระบุชัดว่าแปลง MovieLens เป็นสเกล 10 ด้วยการคูณ 2 หนังหนึ่งเรื่องอาจอยู่หลายแนว ดังนั้นผลรวมจำนวนรีวิวจากหลายแนวจะนับรีวิวซ้ำ และปีใน view คือ **ปีที่หนังฉาย** ไม่ใช่ปีที่ผู้ใช้ให้คะแนน

## ตรวจข้อมูลก่อนสร้างกราฟ

เปิด DBeaver หรือ SQL editor แล้วรัน:

```sql
SELECT count(*) FROM public.powerbi_movies;
SELECT * FROM public.powerbi_overview;
SELECT * FROM public.powerbi_release_year ORDER BY release_year DESC LIMIT 10;
SELECT * FROM public.powerbi_genres ORDER BY rating_count DESC LIMIT 10;
SELECT title, movielens_rating_count, movielens_average_rating,
       imdb_vote_count, imdb_average_rating
FROM public.powerbi_movies
WHERE movielens_rating_count >= 1000 AND imdb_vote_count >= 1000
ORDER BY movielens_average_rating DESC
LIMIT 10;
```

MovieLens 32M ควรได้ `powerbi_movies` 87,585 แถว ค่า IMDb และจำนวนแถวบางชุดขึ้นอยู่กับ snapshot ที่ดาวน์โหลด บันทึกวันที่ดาวน์โหลด IMDb และภาพหน้า Airflow Success, ผล quality report, SQL counts และ dashboard ไว้ประกอบรายงาน
