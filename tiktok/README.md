# Kịch bản TikTok — Săn Đồ Cũ (Crawl đa nguồn: FB Group, Marketplace, Chợ Tốt)

Mục tiêu: Video dọc 9:16, 45–60s. Nội dung cuốn hút, có minh họa màn hình thật, giúp người xem hiểu ngay lợi ích và muốn lưu/share.

---

## 0) Đối tượng & Góc tiếp cận

- Người săn đồ cũ, deal hời, đồ hi-tech/đồ gia dụng, dân dev/side-project.
- Nỗi đau: Mất thời gian lướt nhiều nơi, trùng lặp bài, bỏ lỡ deal ngon.
- Giải pháp: "Săn Đồ Cũ" gom dữ liệu từ 3 nguồn (FB Group, FB Marketplace, Chợ Tốt) → 1 UI tìm/lọc nhanh, chống trùng, đánh dấu đã xem.

---

## 1) Hook (0:00–0:04)

- Voice-over (VO): "Dừng lướt! Muốn săn đồ cũ ngon-bổ-rẻ mà không bỏ lỡ?"
- Text overlay: "Gộp 3 nguồn → 1 chỗ"
- Visual: Cú cắt nhanh 0.5s mỗi cảnh: FB Group → FB Marketplace → Chợ Tốt → Logo/tiêu đề "Săn Đồ Cũ".
- SFX/Music: Nhạc trend nhanh 120–140 BPM, hit “whoosh” khi chuyển cảnh.

---

## 2) Vấn đề (0:04–0:10)

- VO: "Mỗi ngày lướt 3–4 nơi, bài trùng, trôi mất deal." 
- Visual: Quay màn hình lướt group/marketplace/chợ tốt, tua nhanh; chèn emoji 😵‍💫.
- Text overlay: "Trùng bài • Trôi deal • Mất thời gian"

---

## 3) Giải pháp (0:10–0:16)

- VO: "Mình viết tool gom tất cả vào một chỗ, tìm/lọc siêu nhanh."
- Visual: Hiện UI web của dự án (frontend) với danh sách bài, search box, tabs.
- Text overlay: "Săn Đồ Cũ — Thu thập đa nguồn"

---

## 4) Demo nhanh (0:16–0:36)

- Cảnh 1 (0:16–0:22)
  - VO: "Bấm crawl Facebook Marketplace…"
  - Visual: Terminal chạy lệnh:
    - `python backend/login_and_save_state.py`
    - `python backend/main.py 3`
  - Chèn highlight vào các log đang ghi CSV.
  - Text overlay nhỏ: "Tự thêm crawl_time, chống trùng id"

- Cảnh 2 (0:22–0:28)
  - VO: "…hoặc Chợ Tốt theo trang."
  - Visual: Terminal chạy:
    - `python backend/main.py 2 --start 1 --end 3`
  - Cắt nhanh sang thư mục `frontend/public/data/sources/` hiển thị các file CSV.

- Cảnh 3 (0:28–0:36)
  - VO: "Frontend đọc CSV, gộp lại, tìm 'iPhone' và lọc theo nguồn."
  - Visual: UI: nhập 'iPhone' → filter "Marketplace" → click mở post.
  - Overlay: "Đánh dấu đã xem • Gợi ý tag"

---

## 5) Lợi ích chính (0:36–0:48)

- VO: "Một cú tìm cho tất cả nguồn, không sợ trùng bài, đánh dấu để không mất dấu."
- Visual: Check-list bật tắt lần lượt trên màn hình.
- Bullet overlay: "• FB Group (Selenium) • Marketplace (Playwright) • Chợ Tốt (BS4)"

---

## 6) CTA (0:48–0:60)

- VO: "Muốn săn deal nhanh hơn? Comment 'SCRIPT' mình gửi repo, nhớ thả ❤️ và lưu video để không bỏ lỡ!"
- Visual: Con trỏ chuột nhấn nút Star repo (nếu có màn hình GitHub), hoặc banner "Link ở bio / comment ghim".
- Text overlay lớn: "Repo + Hướng dẫn cài đặt ở bio"
- Hashtags gợi ý: `#sannhanh #sandocu #chotot #facebookmarketplace #opensource #crawler #devvietnam #tietkiem #meomua #sideproject`

---

## 7) Bản lời thoại đầy đủ (Teleprompter)

"Dừng lướt! Muốn săn đồ cũ ngon-bổ-rẻ mà không bỏ lỡ? Mỗi ngày phải lướt 3–4 nơi, bài thì trùng, deal thì trôi mất. Mình viết Săn Đồ Cũ: tool gom bài từ Facebook Group, Facebook Marketplace và Chợ Tốt về một chỗ. Chỉ cần chạy crawl một lần, dữ liệu đổ vào CSV, frontend gộp lại, tìm từ khóa và lọc theo nguồn cực nhanh. Quan trọng là chống trùng theo id và có đánh dấu đã xem để bạn không bỏ sót. Muốn săn deal nhanh hơn, comment 'SCRIPT' mình gửi repo và hướng dẫn cài đặt. Nhớ thả tim và lưu video nhé!"

---

## 8) Danh sách cảnh quay chi tiết (Shot list)

1) Hook nói trước máy (0:00–0:04)
   - Khung: Cận mặt 9:16, ánh sáng tốt, nói nhanh – dứt khoát.
   - Nội dung: Câu hook + chỉ tay sang bên trái, overlay chữ.
   - SFX: Whoosh nhẹ khi chuyển.

2) B-roll vấn đề (0:04–0:10)
   - Quay màn hình lướt 3 nguồn; tua nhanh 2x–4x; chèn emoji.

3) Reveal giải pháp (0:10–0:16)
   - Chụp UI frontend: danh sách bài, ô tìm kiếm, tabs nguồn.

4) Terminal – Marketplace (0:16–0:22)
   - Gõ: `python backend/login_and_save_state.py`, rồi `python backend/main.py 3`.
   - Zoom vào log ghi CSV.

5) Terminal – Chợ Tốt (0:22–0:28)
   - Gõ: `python backend/main.py 2 --start 1 --end 3`.
   - Mở thư mục `frontend/public/data/sources/`.

6) UI – Tìm & Lọc (0:28–0:36)
   - Gõ "iPhone" → chọn nguồn "Marketplace" → mở 1 post.

7) UI – Đánh dấu đã xem (0:36–0:40)
   - Click toggle/đánh dấu, chuyển tab Viewed/Unviewed.

8) Lợi ích bullet (0:40–0:48)
   - Overlay 3 bullet tech + dấu tích.

9) CTA + Star repo (0:48–0:56)
   - Màn hình GitHub/bao bì dự án; nhấn Star; hiển thị "Link ở bio".

10) End card (0:56–0:60)
    - Màn hình freeze: tên dự án + hashtag + lời kêu gọi lưu/share.

---

## 9) Phần mô tả/Caption mẫu

"Tool gom dữ liệu đồ cũ từ Facebook Group, Marketplace và Chợ Tốt về một chỗ. Tìm nhanh, lọc theo nguồn, chống trùng, đánh dấu đã xem. Comment 'SCRIPT' mình gửi repo + hướng dẫn cài đặt!"

Hashtags: `#sandocu #chotot #facebookmarketplace #opensource #crawler #devvietnam #sideproject #muanhanh #dealngon #automation`

---

## 10) Gợi ý thumbnail (nếu cần cho cross-post)

- Tiêu đề lớn: "GOM 3 NGUỒN → 1 CHỖ"
- Phụ đề: "Không bỏ lỡ deal ngon"
- Hình: 3 logo/ảnh màn hình mờ của FB Group, Marketplace, Chợ Tốt ở background.
- Màu: Nền tối, chữ vàng/trắng nổi bật.

---

## 11) Ghi chú kỹ thuật (nếu người xem muốn thử)

- Yêu cầu: Python 3.12+ (hoặc 3.14), Node 18+, Chrome, Playwright.
- Cài nhanh (tóm tắt):
  - Lưu session Facebook:
    - `python backend/login_and_save_state.py`
  - Chạy crawler:
    - FB Group: `python backend/main.py 1 --count 20 [--headless]`
    - Chợ Tốt: `python backend/main.py 2 --start 1 --end 3`
    - Marketplace: `python backend/main.py 3`
  - Frontend dev: `cd frontend && npm run dev`

Lưu ý: Tôn trọng điều khoản nền tảng khi thu thập dữ liệu và bảo mật thông tin đăng nhập.

---

## 12) Biến thể 15 giây (dự phòng Reels/Shorts)

- 0:00–0:02: Hook: "Săn đồ cũ 3 nguồn chỉ trong 1 chỗ!"
- 0:02–0:06: Terminal chạy `python backend/main.py 3` + `2 --start 1 --end 3`.
- 0:06–0:10: UI gõ "iPhone" + lọc nguồn.
- 0:10–0:13: Overlay bullet: Chống trùng id • Đánh dấu đã xem.
- 0:13–0:15: CTA: "Comment SCRIPT – Lấy repo ở bio."

---

## 13) Checklist quay (in nhanh)

- [ ] B-roll lướt 3 nguồn (FB Group, Marketplace, Chợ Tốt)
- [ ] Màn hình Terminal chạy lệnh (Marketplace + Chợ Tốt)
- [ ] Thư mục CSV: `frontend/public/data/sources/`
- [ ] UI: Tìm kiếm + Lọc + Đánh dấu đã xem
- [ ] Cảnh nói Hook + CTA
- [ ] End card + Hashtag

