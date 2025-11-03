# Săn Đồ Cũ

Ứng dụng React giúp xem danh sách các món đồ cũ từ file CSV, đánh dấu đã xem và tìm kiếm theo từ khóa. Giao diện theo tab, thân thiện mobile, dùng Tailwind CSS + Headless UI + Heroicons.

## Tính năng

- Đọc dữ liệu từ nhiều CSV nguồn: `public/data/sources/` (Chợ Tốt, Facebook Group, Facebook Market) và gộp hiển thị.
- Hiển thị nhãn nguồn dữ liệu trên từng item, kèm liên kết bài gốc nếu có.
- Bộ lọc theo Nguồn (3 group). (Đã bỏ bộ lọc theo Loại theo yêu cầu.)
- Tìm kiếm theo nhiều từ khóa trong tiêu đề và mô tả (có thể nhập nhiều từ, cách nhau dấu phẩy hoặc khoảng trắng).
- Đánh dấu “đã xem” từng món (lưu vào `localStorage`).
- Tabs: Tất cả / Chưa xem / Đã xem.

## Bắt đầu

1. Cài đặt phụ thuộc:

   ```bash
   npm install
   ```

2. Chạy dev:

   ```bash
   npm run dev
   ```

   Sau đó mở địa chỉ hiển thị trên terminal (mặc định: `http://localhost:5173`).

3. Build production:

   ```bash
   npm run build && npm run preview
   ```

## Cấu trúc chính

- `index.html`: Mốc khởi đầu.
- `src/App.tsx`: Logic giao diện, đọc nhiều CSV, lọc theo nguồn/loại, tìm kiếm, đánh dấu đã xem.
- `public/data/sources/`: Chứa các file CSV nguồn (ví dụ: `chotot_*.csv`, `facebook_group_*.csv`, `facebook_marketplace_*.csv`).
- `src/index.css`: Khởi tạo Tailwind.

## Định dạng CSV

Tối thiểu gồm cột: `id,title,description,price,location` và nếu có thêm: `seller,post_url,image` sẽ được hiển thị. Ứng dụng có xử lý mềm dẻo một số biến thể tên cột (ví dụ `desc`, `city`, `name`, `url`, `thumbnail`…), và tự tạo ID dự phòng nếu thiếu (đảm bảo không trùng giữa các nguồn).

Phân loại “Loại” được suy luận đơn giản bằng từ khóa trong tiêu đề/mô tả (Camera/Lens/Phone/Laptop/Accessory/Other).

## Ghi chú bảo trì

- Tách logic: `useCSV`, `useViewed`, lọc theo từ khóa bằng `useMemo` để dễ đọc và mở rộng.
- Tailwind tuân thủ chuẩn v3, có sẵn `line-clamp`. Heroicons/Headless UI dùng cho icon và Tabs.
- Có thể thay đổi quy tắc tìm kiếm (AND/OR) trong `filtered` tại `src/App.tsx`.
