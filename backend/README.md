Xem tài liệu tổng quan dự án tại `README.md` ở thư mục gốc.

Một số điểm riêng của backend:
- CLI: `backend/main.py` — chọn crawler và tham số.
- Crawler:
  - `facebook_marketplace_crawler.py` (Playwright)
  - `facebook_group_crawler.py` (Selenium)
  - `chotot_crawler.py` (requests/BS4)
- Lưu session Facebook: `backend/login_and_save_state.py`
- Writer CSV thống nhất: `backend/utils/csv_writer.py` (append + chống trùng + tự thêm `crawl_time`), ghi vào `frontend/public/data/sources/`.
