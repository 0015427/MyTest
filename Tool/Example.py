from MySQLScript import MySQLBatchProcessor, UniversalBatchInserter
from Tool.AbandonedClass import MultiTableDataGenerator

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
                    {'name': 'id', 'type': 'VARCHAR(32)'},
                    {'name': 'username', 'type': 'VARCHAR(50)'},
                    {'name': 'email', 'type': 'VARCHAR(100)'}
                ],
                'foreign_keys': []
            },
            'orders': {
                'columns': [
                    {'name': 'id', 'type': 'VARCHAR(32)'},
                    {'name': 'user_id', 'type': 'VARCHAR(32)'},
                    {'name': 'order_date', 'type': 'DATE'},
                    {'name': 'amount', 'type': 'DECIMAL(10,2)'}
                ],
                'foreign_keys': [
                    {'column': 'user_id', 'references_table': 'users'}
                ]
            },
            'order_items': {
                'columns': [
                    {'name': 'id', 'type': 'VARCHAR(32)'},
                    {'name': 'order_id', 'type': 'VARCHAR(32)'},
                    {'name': 'product_name', 'type': 'VARCHAR(100)'},
                    {'name': 'quantity', 'type': 'INT'}
                ],
                'foreign_keys': [
                    {'column': 'order_id', 'references_table': 'orders'}
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

def example_with_custom_data_generation(processor: MySQLBatchProcessor):
    """
    使用通用批量插入工具的示例
    """

    # 用户自定义数据生成逻辑
    users_data = []
    for i in range(1000):
        user_id = str(uuid.uuid4()).replace('-', '')
        users_data.append((user_id, f'username_{i}', f'email_{i}@example.com'))

    orders_data = []
    for i in range(3000):
        order_id = str(uuid.uuid4()).replace('-', '')
        # 从已生成的用户数据中选择一个ID
        user_id = users_data[i % len(users_data)][0]  # 关联到用户
        orders_data.append((order_id, user_id, '2023-01-01', round(random.uniform(10, 1000), 2)))

    order_items_data = []
    for i in range(9000):
        item_id = str(uuid.uuid4()).replace('-', '')
        # 从已生成的订单数据中选择一个ID
        order_id = orders_data[i % len(orders_data)][0]  # 关联到订单
        order_items_data.append(
            (item_id, order_id, f'product_{i}', random.randint(1, 10), round(random.uniform(5, 500), 2)))

    # 配置表结构
    table_configs = [
        {
            'table_name': 'users',
            'columns': ['id', 'username', 'email'],
            'data': users_data,
            'dependencies': []  # 没有依赖
        },
        {
            'table_name': 'orders',
            'columns': ['id', 'user_id', 'order_date', 'amount'],
            'data': orders_data,
            'dependencies': [
                {
                    'column': 'user_id',
                    'parent_table': 'users'
                }
            ]
        },
        {
            'table_name': 'order_items',
            'columns': ['id', 'order_id', 'product_name', 'quantity', 'price'],
            'data': order_items_data,
            'dependencies': [
                {
                    'column': 'order_id',
                    'parent_table': 'orders'
                }
            ]
        }
    ]

    # 使用通用批量插入工具
    inserter = UniversalBatchInserter(processor)
    success = inserter.batch_insert_related_tables(
        table_data_configs=table_configs,
        batch_size=5000,
        show_progress=True
    )

    if success:
        print("批量插入成功")
    else:
        print("批量插入失败")


def example_with_per_table_batch_size(processor: MySQLBatchProcessor):
    """
    使用每个表独立批量大小的示例
    """
    # 生成数据（假设1对N关系：1个用户对应3个订单，1个订单对应3个商品项）
    users_data = []
    for i in range(1000):
        user_id = str(uuid.uuid4()).replace('-', '')
        users_data.append((user_id, f'username_{i}', f'email_{i}@example.com'))

    orders_data = []
    for i in range(3000):  # 3倍于用户数
        order_id = str(uuid.uuid4()).replace('-', '')
        user_id = users_data[i % len(users_data)][0]
        orders_data.append((order_id, user_id, '2023-01-01', round(random.uniform(10, 1000), 2)))

    order_items_data = []
    for i in range(9000):  # 3倍于订单数
        item_id = str(uuid.uuid4()).replace('-', '')
        order_id = orders_data[i % len(orders_data)][0]
        order_items_data.append(
            (item_id, order_id, f'product_{i}', random.randint(1, 10), round(random.uniform(5, 500), 2)))

    # 配置表结构，每个表有自己的批量大小
    table_configs = [
        {
            'table_name': 'users',
            'columns': ['id', 'username', 'email'],
            'data': users_data,
            'dependencies': [],
            'batch_size': 500,  # 用户表较小批量
            'relationship_multiplier': 0.5  # 可选：关系乘数
        },
        {
            'table_name': 'orders',
            'columns': ['id', 'user_id', 'order_date', 'amount'],
            'data': orders_data,
            'dependencies': [{'column': 'user_id', 'parent_table': 'users'}],
            'batch_size': 1500,  # 订单表中等批量
            'relationship_multiplier': 1.5
        },
        {
            'table_name': 'order_items',
            'columns': ['id', 'order_id', 'product_name', 'quantity', 'price'],
            'data': order_items_data,
            'dependencies': [{'column': 'order_id', 'parent_table': 'orders'}],
            'batch_size': 4500,  # 商品项表大批量
            'relationship_multiplier': 3.0
        }
    ]

    # 使用通用批量插入工具
    inserter = UniversalBatchInserter(**processor.config)

    # 使用基础版本
    success = inserter.batch_insert_related_tables(
        table_data_configs=table_configs,
        batch_size=1000,  # 这个值会被各表的batch_size覆盖
        show_progress=True
    )

    # 或使用高级版本
    # success = inserter.batch_insert_related_tables_advanced(
    #     table_data_configs=table_configs,
    #     base_batch_size=1000,
    #     show_progress=True
    # )

    if success:
        print("批量插入成功")
    else:
        print("批量插入失败")


if __name__ == '__main__':

    sql_processor = MySQLBatchProcessor(
        host='localhost',
        port=3306,
        user='root',
        password='123456',
        database='performance_db',
        auto_optimize=True
    )

    example_with_custom_data_generation(sql_processor)

    # plan_multi_table_example(sql_processor)


    # date = generate_test_data(100000)
    # save_data_to_csv(date, 'data.csv')
    # MySQLBatchProcessor()