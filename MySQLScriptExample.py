from typing import List, Tuple, Any, Optional
import logging
import time
from tqdm import tqdm
import random
import string
from MySQLScript import MySQLBatchProcessor, UniversalBatchInserter
import uuid
from datetime import datetime



def generate_test_data(count: int) -> List[Tuple[Any]]:
    """
    生成测试数据

    Args:
        count: 数据条数

    Returns:
        List[Tuple[Any]]: 测试数据列表
    """
    data = []
    process_bar = tqdm(total=count, desc="生成数据进度", disable= False,
                                ncols=100, leave=False)
    for i in range(count):
        # 生成id
        id = str(uuid.uuid4()).replace('-', '')
        # 生成随机用户名
        username = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        # 生成随机年龄(18-80)
        age = random.randint(18, 80)
        # 生成随机邮箱
        email = f"{username}@example.com"
        # 生成随机城市
        cities = ['北京', '上海', '广州', '深圳', '杭州', '南京', '武汉', '成都']
        city = random.choice(cities)
        # 生成时间
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        data.append((id, username, age, email, city, created_at))
        process_bar.update()
    process_bar.close()
    return data


def create_test_table(processor: MySQLBatchProcessor):
    """
    创建测试表

    Args:
        processor: MySQLBatchProcessor实例
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_users (
    id VARCHAR(64) NOT NULL,  -- 增加长度
    username VARCHAR(50) NOT NULL,
    age INT NOT NULL,
    email VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    cursor = processor.connection.cursor()
    try:
        cursor.execute(create_table_sql)
        processor.connection.commit()
        logging.info("测试表创建成功")
    except Exception as e:
        logging.error(f"创建测试表失败: {e}")
    finally:
        cursor.close()


def save_data_to_csv(data_list: List[Tuple[Any]], csv_file_path: str
                     , column_names: Optional[List[str]] = None) -> str:
    """
    将数据保存为CSV文件，右键对应表，导入CSV文件

    Args:
        data_list: 数据列表
        csv_file_path: CSV文件路径
        column_names: 列名列表(可选)

    Returns:
        str: CSV文件路径
    """
    try:
        # 创建进度条
        progress_bar = tqdm(total=len(data_list), desc="生成CSV文件", ncols=100)

        # 保存数据到CSV文件
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            if column_names is not None:
                writer.writerow(column_names)
            for i, row in enumerate(data_list):
                writer.writerow(row)
                # 每1000行更新一次进度条
                if (i + 1) % 1000 == 0:
                    progress_bar.update(1000)

            # 更新剩余的进度
            remaining = len(data_list) % 1000
            if remaining > 0:
                progress_bar.update(remaining)

        progress_bar.close()
        logging.info(f"CSV文件生成完成: {csv_file_path}")
        return csv_file_path

    except Exception as e:
        logging.error(f"生成CSV文件失败: {e}")
        raise


def generate_sql_script(data_list: List[Tuple[Any]], sql_file_path: str,
                        table_name: str, columns: List[str]) -> str:
    """
    生成SQL脚本文件，使用导入功能

    Args:
        data_list: 数据列表
        sql_file_path: SQL文件路径
        table_name: 表名
        columns: 列名列表

    Returns:
        str: SQL文件路径
    """
    try:
        # 创建进度条
        progress_bar = tqdm(total=len(data_list), desc="生成SQL脚本", ncols=100)

        # 写入SQL脚本文件
        with open(sql_file_path, 'w', encoding='utf-8') as sql_file:
            # 写入初始化设置
            sql_file.write("-- SQL脚本用于批量插入数据\n")
            sql_file.write("SET autocommit=0;\n")
            sql_file.write("SET unique_checks=0;\n")
            sql_file.write("SET foreign_key_checks=0;\n")
            sql_file.write("START TRANSACTION;\n\n")

            # 分批生成INSERT语句
            batch_size = 10000
            for i in range(0, len(data_list), batch_size):
                batch_data = data_list[i:i + batch_size]

                # 写入批次开始标记
                sql_file.write(
                    f"-- 批次 {i // batch_size + 1}: 记录 {i + 1} 到 {min(i + len(batch_data), len(data_list))}\n")

                # 为这一批次生成INSERT语句
                values_list = []
                for row in batch_data:
                    # 处理特殊字符和引号
                    escaped_row = []
                    for value in row:
                        if isinstance(value, str):
                            # 转义单引号
                            escaped_value = value.replace("'", "''")
                            escaped_row.append(f"'{escaped_value}'")
                        elif value is None:
                            escaped_row.append('NULL')
                        else:
                            escaped_row.append(str(value))
                    values_list.append(f"({','.join(escaped_row)})")

                # 写入完整的INSERT语句
                columns_str = ', '.join([f"`{col}`" for col in columns])
                insert_statement = f"INSERT INTO `{table_name}` ({columns_str}) VALUES \n"
                insert_statement += ",\n".join(values_list) + ";\n"
                sql_file.write(insert_statement)

                # 每100批提交一次事务
                if (i // batch_size + 1) % 100 == 0:
                    sql_file.write("COMMIT;\n")
                    sql_file.write("START TRANSACTION;\n")

                # 更新进度条
                progress_bar.update(len(batch_data))

            # 写入最终提交
            sql_file.write("COMMIT;\n")
            sql_file.write("-- 数据生成完成\n")

        progress_bar.close()
        logging.info(f"SQL脚本生成完成: {sql_file_path}")
        return sql_file_path

    except Exception as e:
        logging.error(f"生成SQL脚本失败: {e}")
        raise

# plan two use LOAD DATA INFILE
import  csv
import os

def save_data_to_secure_directory(data_list: List[Tuple[Any]], csv_file_path: str ,processor: MySQLBatchProcessor) -> str:
    """将数据保存到MySQL允许的安全目录"""
    # 获取安全目录路径
    secure_dir = get_secure_file_priv(processor)

    if not secure_dir:
        raise Exception("无法获取secure_file_priv设置")

    if secure_dir == "":
        # 如果为空字符串，表示没有限制，可以使用临时目录
        filename = os.path.join(os.getcwd(), csv_file_path)
    else:
        # 使用安全目录
        filename = os.path.join(secure_dir, csv_file_path)

    # 保存数据到CSV文件
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data_list)

    return filename


def get_secure_file_priv(processor: MySQLBatchProcessor) -> str:
    """获取MySQL的secure-file-priv设置"""
    try:
        result = processor.execute_query("SHOW VARIABLES LIKE 'secure_file_priv'")
        if result:
            secure_dir = result[0][1] if len(result[0]) > 1 else ""
            print(f"MySQL secure-file-priv设置: '{secure_dir}'")
            return secure_dir
        else:
            print("无法获取secure-file-priv设置")
            return ""
    except Exception as e:
        logging.error(f"检查secure-file-priv设置失败: {e}")
        return ""


def plan_two(processor: MySQLBatchProcessor):
    """
    使用LOAD DATA INFILE导入数据  #难搞，要权限 #或者拿生成的CSV去手动导入
    :param processor:
    :return:
    """

    try:
        # 连接数据库
        if not processor.connect():
            print("数据库连接失败")
            exit(1)

        # 创建测试表
        create_test_table(processor)

        # 生成测试数据并保存为CSV
        generate_data_count = 10000000
        print(f"正在生成{generate_data_count / 10000}万条测试数据...")
        start_time = time.time()
        test_data = generate_test_data(generate_data_count)
        generate_time = time.time() - start_time
        print(f"数据生成完成，耗时: {generate_time:.2f}秒")

        csv_filename = "test_data.csv"
        save_data_to_secure_directory(test_data, csv_filename, processor)

        # 快速导入数据
        start_time = time.time()
        success = processor.load_data_from_file('test_users', csv_filename, True)
        load_time = time.time() - start_time
        print(f"LOAD DATA INFILE 结果: {'成功' if success else '失败'}")
        print(f"导入耗时: {load_time:.2f}秒")
        print(f"平均每秒插入: {generate_data_count / load_time:.0f}条记录")

    except Exception as e:
        logging.error(f"数据导入失败: {e}")
    finally:
        # 关闭连接
        processor.disconnect()


def plan_one(processor: MySQLBatchProcessor):
    """
    多线程使用批量插入插入数据
    :param processor:
    :return:
    """
    # plan one
    try:
        # 连接数据库
        if not processor.connect():
            print("数据库连接失败")
            exit(1)

        # 创建测试表
        create_test_table(processor)

        # 生成测试数据
        generate_data_count = 10000000
        print(f"正在生成{generate_data_count / 10000}万条测试数据...")
        start_time = time.time()
        test_data = generate_test_data(generate_data_count)
        generate_time = time.time() - start_time
        print(f"数据生成完成，耗时: {generate_time:.2f}秒")

        # 批量插入数据并显示进度条
        print(f"开始插入{generate_data_count / 10000}万条数据...")
        start_time = time.time()
        is_success = processor.batch_insert(
            table_name='test_users',
            columns=['username', 'age', 'email', 'city'],
            data_list=test_data,
            batch_size=10000,  # 调整批次大小
            show_progress=True,  # 显示进度条
            use_multithreading=True,  # 启用多线程
            max_workers=8  # 设置最大线程数
        )
        insert_time = time.time() - start_time
        print(f"批量插入结果: {'成功' if is_success else '失败'}")
        print(f"插入耗时: {insert_time:.2f}秒")
        print(f"平均每秒插入: {generate_data_count / insert_time:.0f}条记录")

    except Exception as e:
        logging.error(f"测试过程中出现错误: {e}")
    finally:
        # 关闭连接
        processor.disconnect()


def plan_three(processor: MySQLBatchProcessor):
    """
    生成SQL脚本文件用于后续手动导入
    :param processor: MySQLBatchProcessor实例
    """
    try:
        # 生成测试数据
        generate_data_count = 10000000
        print(f"正在生成{generate_data_count / 10000}万条测试数据...")
        start_time = time.time()
        test_data = generate_test_data(generate_data_count)
        generate_time = time.time() - start_time
        print(f"数据生成完成，耗时: {generate_time:.2f}秒")

        # 生成SQL脚本文件
        sql_filename = "insert_test_data.sql"
        print(f"正在生成SQL脚本文件: {sql_filename}")

        start_time = time.time()

        sql_filepath = generate_sql_script(
            data_list=test_data,
            sql_file_path=sql_filename,
            table_name='test_users',
            columns=['username', 'age', 'email', 'city']
        )

        generate_sql_time = time.time() - start_time
        file_size = os.path.getsize(sql_filename) / (1024 * 1024)  # MB

        print(f"SQL脚本生成完成，耗时: {generate_sql_time:.2f}秒")
        print(f"SQL脚本文件大小: {file_size:.2f} MB")
        print(f"文件位置: {os.path.abspath(sql_filepath)}")
        print("\n使用方法:")
        print("1. 登录MySQL命令行:")
        print("   mysql -u root -p performance_db")
        print("2. 执行SQL脚本:")
        print(f"   source {os.path.abspath(sql_filepath)}")

    except Exception as e:
        logging.error(f"生成SQL脚本失败: {e}")
    finally:
        # 断开连接
        processor.disconnect()


def plan_four(processor: MySQLBatchProcessor):
    """
    生成本地CSV文件以便手动导入
    :param processor: MySQLBatchProcessor实例
    """
    try:
        # 生成测试数据
        generate_data_count = 10000
        print(f"正在生成{generate_data_count / 10000}万条测试数据...")
        start_time = time.time()
        test_data = generate_test_data(generate_data_count)
        generate_time = time.time() - start_time
        print(f"数据生成完成，耗时: {generate_time:.2f}秒")

        # 生成CSV文件
        csv_filename = "test_data_for_manual_import.csv"
        print(f"正在生成CSV文件: {csv_filename}")

        start_time = time.time()
        csv_filepath = save_data_to_csv(test_data, csv_filename)
        generate_csv_time = time.time() - start_time
        file_size = os.path.getsize(csv_filename) / (1024 * 1024)  # MB

        print(f"CSV文件生成完成，耗时: {generate_csv_time:.2f}秒")
        print(f"CSV文件大小: {file_size:.2f} MB")
        print(f"文件位置: {os.path.abspath(csv_filepath)}")
        print("\n手动导入方法:")
        print("1. 打开MySQL客户端工具(如Navicat、MySQL Workbench等)")
        print("2. 右键点击目标表 'test_users'")
        print("3. 选择 '导入向导' 或类似选项")
        print("4. 选择生成的CSV文件")
        print("5. 配置导入选项:")
        print("6. 先选这CSV文件")
        print("7. 改变，选择EXCEL填写友好配置")
        print("8. 字符集选语句CHARSET，一般utf8mb4")
        print("9. 点击导入")

    except Exception as e:
        logging.error(f"生成CSV文件失败: {e}")
    finally:
        # 断开连接
        processor.disconnect()


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


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # 创建处理器实例
    sql_processor = MySQLBatchProcessor(
        host='localhost',
        port=3306,
        user='root',
        password='123456',
        database='performance_db',
        auto_optimize=True
    )

    # 单表插入方式
    # plan_one(sql_processor)

    # 多关联表插入方式（如果可以更推荐导入CSV，可以查看数据，更直观）
    # example_with_per_table_batch_size(sql_processor)

    # CSV导入
    plan_four(sql_processor)