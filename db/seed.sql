DROP TABLE IF EXISTS relationships CASCADE;
DROP TABLE IF EXISTS entities CASCADE;

-- Bảng entities: lưu trữ các thành phần hệ thống (hàm, luồng, quy tắc, thiết kế)
CREATE TABLE entities (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    name TEXT NOT NULL,
    detail TEXT
);

-- Bảng relationships: lưu trữ mối quan hệ có hướng giữa các entities
CREATE TABLE relationships (
    from_id INT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    to_id INT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    rel_type TEXT NOT NULL,
    PRIMARY KEY (from_id, to_id, rel_type)
);

-- Tạo Index để tăng tốc độ tìm kiếm theo tên entity
CREATE INDEX idx_entities_name ON entities(name);
CREATE INDEX idx_entities_type ON entities(type);

-- 1. Chèn dữ liệu Entities
INSERT INTO entities (id, type, name, detail) VALUES
(1, 'function', 'transfer', 'Hàm xử lý trừ tiền và điều chuyển số dư tài khoản'),
(2, 'function', 'publish', 'Hàm phát tán event thông báo giao dịch lên Message Broker'),
(3, 'function', 'deque', 'Hàm tiêu thụ và xử lý tác vụ từ hàng đợi ngầm'),
(4, 'flow', 'chuyen-tien', 'Luồng nghiệp vụ xử lý giao dịch chuyển tiền trực tuyến'),
(5, 'rule', 'R-023', 'Chuyển tiền > 50 triệu phải xác thực KYC bậc 2'),
(6, 'rule', 'R-045', 'Sau khi trừ tiền phải publish sự kiện để đối soát');

SELECT setval('entities_id_seq', (SELECT MAX(id) FROM entities));

-- 2. Chèn dữ liệu Relationships
-- 3 hàm (transfer, publish, deque) thuộc luồng chuyen-tien (belongs_to)
INSERT INTO relationships (from_id, to_id, rel_type) VALUES
(1, 4, 'belongs_to'),
(2, 4, 'belongs_to'),
(3, 4, 'belongs_to');

-- Luồng chuyen-tien bắt buộc phải tuân thủ cả 2 quy tắc R-023 và R-045 (must_follow)
INSERT INTO relationships (from_id, to_id, rel_type) VALUES
(4, 5, 'must_follow'),
(4, 6, 'must_follow');
