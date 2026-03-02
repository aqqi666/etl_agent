"""初始化测试数据库：创建 source_db（含 orders + region_mapping）和空的 test_db"""

from sqlalchemy import create_engine, text

CONN = "mysql+pymysql://root:test123@127.0.0.1:3307/?charset=utf8mb4"


def main():
    engine = create_engine(CONN)
    with engine.connect() as conn:
        print("重建 source_db ...")
        conn.execute(text("DROP DATABASE IF EXISTS source_db"))
        conn.execute(text("CREATE DATABASE source_db CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"))

        print("重建 test_db ...")
        conn.execute(text("DROP DATABASE IF EXISTS test_db"))
        conn.execute(text("CREATE DATABASE test_db CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"))

        print("创建 orders 表 ...")
        conn.execute(text("""
            CREATE TABLE source_db.orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                order_no VARCHAR(32),
                customer_name VARCHAR(64),
                product VARCHAR(64),
                quantity INT,
                unit_price DECIMAL(10,2),
                order_date DATE,
                status VARCHAR(16),
                region VARCHAR(32)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        print("创建 region_mapping 维表 ...")
        conn.execute(text("""
            CREATE TABLE source_db.region_mapping (
                region_code VARCHAR(32) PRIMARY KEY,
                region_name VARCHAR(64),
                area VARCHAR(32)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        print("插入 orders 数据 (13行，含异常数据) ...")
        conn.execute(text("""
            INSERT INTO source_db.orders
                (order_no, customer_name, product, quantity, unit_price, order_date, status, region)
            VALUES
                ('ORD001', '张三', '笔记本电脑', 2, 5999.00, '2024-01-15', 'completed', '华东'),
                ('ORD002', '李四', '机械键盘',   5,  399.00, '2024-01-16', 'completed', '华北'),
                ('ORD003', '王五', '显示器',     1, 2499.00, '2024-01-17', 'pending',   '华南'),
                ('ORD004', '赵六', '鼠标',      10,  129.00, '2024-01-18', 'completed', '华东'),
                ('ORD005', '张三', '耳机',       3,  899.00, '2024-01-19', 'cancelled', '华东'),
                ('ORD006', '李四', '笔记本电脑', 1, 5999.00, '2024-01-20', 'completed', '华北'),
                ('ORD007', '孙七', '平板电脑',   2, 3299.00, '2024-01-21', 'pending',   '西南'),
                ('ORD008', '周八', '显示器',     3, 2499.00, '2024-01-22', 'completed', '华东'),
                ('ORD009', '吴九', '机械键盘',   2,  399.00, '2024-01-23', 'completed', '华南'),
                ('ORD010', '郑十', '笔记本电脑', 1, 5999.00, '2024-01-24', 'pending',   '华北'),
                ('ORD011', NULL,   '服务器',     1, 29999.00,'2024-01-25', 'completed', '华东'),
                ('ORD012', '钱十二','交换机',    NULL, 8999.00,'2024-01-26', 'pending',   '西北'),
                ('ORD013', '孙十三','路由器',    -5,   599.00,'2024-01-27', 'completed', '华南')
        """))

        print("插入 region_mapping 数据 (4行) ...")
        conn.execute(text("""
            INSERT INTO source_db.region_mapping (region_code, region_name, area) VALUES
                ('华东', '华东地区', '东部'),
                ('华北', '华北地区', '北部'),
                ('华南', '华南地区', '南部'),
                ('西南', '西南地区', '西部')
        """))

        conn.commit()

    # 验证
    engine2 = create_engine("mysql+pymysql://root:test123@127.0.0.1:3307/source_db?charset=utf8mb4")
    with engine2.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        print(f"\norders 表: {rows} 行")
        rows = conn.execute(text("SELECT COUNT(*) FROM region_mapping")).scalar()
        print(f"region_mapping 表: {rows} 行")

        print("\n异常数据说明:")
        print("  ORD011: customer_name 为 NULL")
        print("  ORD012: quantity 为 NULL, region='西北' 无维表映射")
        print("  ORD013: quantity=-5 负数异常值")

        sample = conn.execute(text("SELECT order_no, customer_name, quantity, region FROM orders WHERE id > 10"))
        for r in sample:
            print(f"  {r[0]}: customer={r[1]}, qty={r[2]}, region={r[3]}")

    print("\n数据库初始化完成!")


if __name__ == "__main__":
    main()
