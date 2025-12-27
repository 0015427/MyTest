from MySQLScript import MySQLBatchProcessor, MultiTableDataGenerator

import random
import uuid
from datetime import datetime

import logging, time


def plan_batch_insert_with_relationships(processor: MySQLBatchProcessor):
    """
    使用batch_insert_with_relationships方法处理关联表
    """
    try:
        if not processor.connect():
            print("数据库连接失败")
            return False

        # 创建表结构
        create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            email VARCHAR(100) NOT NULL
        )
        """

        create_orders_table = """
        CREATE TABLE IF NOT EXISTS orders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT,
            order_date DATE,
            total_amount DECIMAL(10,2)
        )
        """

        create_order_items_table = """
        CREATE TABLE IF NOT EXISTS order_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT,
            product_name VARCHAR(100),
            quantity INT,
            price DECIMAL(10,2)
        )
        """

        cursor = processor.connection.cursor()
        cursor.execute(create_users_table)
        cursor.execute(create_orders_table)
        cursor.execute(create_order_items_table)
        processor.connection.commit()
        cursor.close()

        # 准备表配置，定义依赖关系
        table_configs = [
            {
                'table_name': 'users',
                'columns': ['username', 'email'],
                'data': [('user' + str(i), f'user{i}@example.com') for i in range(100)],
                'id_mapping_key': 'users'  # 标识这个表的ID映射键
            },
            {
                'table_name': 'orders',
                'columns': ['user_id', 'order_date', 'total_amount'],
                'data': [(i % 100 + 1, '2023-01-01', round(random.uniform(10, 1000), 2)) for i in range(300)],
                'foreign_key_mapping': [
                    {
                        'column_index': 0,  # user_id在数据元组中的索引
                        'parent_mapping_key': 'users'  # 引用的父表ID映射键
                    }
                ],
                'id_mapping_key': 'orders'
            },
            {
                'table_name': 'order_items',
                'columns': ['order_id', 'product_name', 'quantity', 'price'],
                'data': [(i % 300 + 1, f'product_{i}', random.randint(1, 10), round(random.uniform(5, 500), 2)) for i in
                         range(900)],
                'foreign_key_mapping': [
                    {
                        'column_index': 0,  # order_id在数据元组中的索引
                        'parent_mapping_key': 'orders'  # 引用的父表ID映射键
                    }
                ]
            }
        ]

        # 批量插入关联数据
        print("正在批量插入关联表数据...")
        start_time = time.time()
        success = processor.batch_insert_with_relationships(
            table_configs=table_configs,
            batch_size=1000,
            show_progress=True
        )

        insert_time = time.time() - start_time
        print(f"关联表插入结果: {'成功' if success else '失败'}")
        print(f"插入耗时: {insert_time:.2f}秒")

        return success

    except Exception as e:
        logging.error(f"关联表数据处理过程中出现错误: {e}")
        return False
    finally:
        processor.disconnect()


def plan_multi_table_example(processor: MySQLBatchProcessor):
    """
    多表关联数据生成和插入示例
    """
    try:
        if not processor.connect():
            print("数据库连接失败")
            return False

        # 定义表结构配置
        schema_config = {
            'users': {
                'columns': [
                    {'name': 'id', 'type': 'INT', 'extra': 'AUTO_INCREMENT'},
                    {'name': 'username', 'type': 'VARCHAR(50)'},
                    {'name': 'email', 'type': 'VARCHAR(100)'}
                ],
                'foreign_keys': []  # users表没有外键
            },
            'orders': {
                'columns': [
                    {'name': 'id', 'type': 'INT', 'extra': 'AUTO_INCREMENT'},
                    {'name': 'user_id', 'type': 'INT'},
                    {'name': 'order_date', 'type': 'DATE'},
                    {'name': 'amount', 'type': 'DECIMAL(10,2)'}
                ],
                'foreign_keys': [
                    {'column': 'user_id', 'references_table': 'users'}  # orders对users是1对n
                ]
            },
            'order_items': {
                'columns': [
                    {'name': 'id', 'type': 'INT', 'extra': 'AUTO_INCREMENT'},
                    {'name': 'order_id', 'type': 'INT'},
                    {'name': 'product_name', 'type': 'VARCHAR(100)'},
                    {'name': 'quantity', 'type': 'INT'}
                ],
                'foreign_keys': [
                    {'column': 'order_id', 'references_table': 'orders'}  # order_items对orders是1对n
                ]
            }
        }

        # 定义每张表需要生成的记录数
        record_counts = {
            'users': 1000,
            'orders': 3000,  # 每个用户平均3个订单
            'order_items': 9000  # 每个订单平均3个商品
        }

        # 创建数据生成器
        generator = MultiTableDataGenerator(processor)

        # 生成关联数据
        print("正在生成多表关联数据...")
        start_time = time.time()
        generated_data = generator.generate_related_data(schema_config, record_counts)
        generate_time = time.time() - start_time
        print(f"数据生成完成，耗时: {generate_time:.2f}秒")

        # 批量插入数据
        print("正在插入多表关联数据...")
        start_time = time.time()
        success = generator.insert_related_data(generated_data, schema_config, batch_size=5000)

        insert_time = time.time() - start_time
        print(f"多表插入结果: {'成功' if success else '失败'}")
        print(f"插入耗时: {insert_time:.2f}秒")

        return success

    except Exception as e:
        logging.error(f"多表数据处理过程中出现错误: {e}")
        return False
    finally:
        processor.disconnect()


if __name__ == '__main__':

    sql_processor = MySQLBatchProcessor(
        host='localhost',
        port=3306,
        user='root',
        password='123456',
        database='performance_db',
        auto_optimize=True
    )

    plan_multi_table_example(sql_processor)


    # date = generate_test_data(100000)
    # save_data_to_csv(date, 'data.csv')
    # MySQLBatchProcessor()