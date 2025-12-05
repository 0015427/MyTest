import pymysql
from typing import List, Tuple, Any, Optional
import logging
import time
from tqdm import tqdm
import random
import string
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class MySQLBatchProcessor:
    """
    MySQL批量数据处理工具类
    支持批量插入、更新、删除等操作
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str,
                 charset: str = 'utf8mb4'):
        """
        初始化数据库连接参数

        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 用户名
            password: 密码
            database: 数据库名
            charset: 字符集
        """
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': charset
        }
        self.connection = None

    def connect(self) -> bool:
        """
        建立数据库连接

        Returns:
            bool: 连接是否成功
        """
        try:
            self.connection = pymysql.connect(**self.config)
            return True
        except Exception as e:
            logging.error(f"数据库连接失败: {e}")
            return False

    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def batch_execute(self, sql: str, data_list: List[Tuple[Any]],
                      batch_size: int = 1000, show_progress: bool = False,
                      use_multithreading: bool = False, max_workers: int = 4) -> bool:
        """
        批量执行SQL语句

        Args:
            sql: SQL模板语句
            data_list: 数据列表，每个元素是一个元组
            batch_size: 每批处理的数据量
            show_progress: 是否显示进度条
            use_multithreading: 是否使用多线程
            max_workers: 最大线程数

        Returns:
            bool: 执行是否成功
        """
        if use_multithreading:
            return self._batch_execute_multithreaded(sql, data_list, batch_size, show_progress, max_workers)
        else:
            return self._batch_execute_single_threaded(sql, data_list, batch_size, show_progress)

    def _batch_execute_single_threaded(self, sql: str, data_list: List[Tuple[Any]],
                                       batch_size: int, show_progress: bool) -> bool:
        """
        单线程批量执行SQL语句

        Args:
            sql: SQL模板语句
            data_list: 数据列表
            batch_size: 每批处理的数据量
            show_progress: 是否显示进度条

        Returns:
            bool: 执行是否成功
        """
        if not self.connection:
            if not self.connect():
                return False

        cursor = self.connection.cursor()
        is_success = True

        try:
            # 分批处理数据
            total_batches = (len(data_list) + batch_size - 1) // batch_size
            progress_bar = tqdm(total=len(data_list), desc="处理进度", disable=not show_progress,
                                ncols=100, leave=False)

            for i in range(0, len(data_list), batch_size):
                batch_data = data_list[i:i + batch_size]
                cursor.executemany(sql, batch_data)
                self.connection.commit()
                progress_bar.update(len(batch_data))
                logging.debug(f"已处理 {min(i + batch_size, len(data_list))}/{len(data_list)} 条记录")

            progress_bar.close()

        except Exception as e:
            logging.error(f"批量执行失败: {e}")
            self.connection.rollback()
            is_success = False
            if 'progress_bar' in locals():
                progress_bar.close()
        finally:
            cursor.close()

        return is_success

    def _batch_execute_multithreaded(self, sql: str, data_list: List[Tuple[Any]],
                                     batch_size: int, show_progress: bool, max_workers: int) -> bool:
        """
        多线程批量执行SQL语句

        Args:
            sql: SQL模板语句
            data_list: 数据列表
            batch_size: 每批处理的数据量
            show_progress: 是否显示进度条
            max_workers: 最大线程数

        Returns:
            bool: 执行是否成功
        """
        # 将数据分割成多个批次
        batches = [data_list[i:i + batch_size] for i in range(0, len(data_list), batch_size)]

        progress_bar = tqdm(total=len(data_list), desc="多线程处理进度", disable=not show_progress,
                            ncols=100, leave=False)

        is_success = True
        lock = threading.Lock()

        def process_batch(batch_data):
            """处理单个批次的数据"""
            local_connection = None
            local_cursor = None
            try:
                # 每个线程创建独立的数据库连接
                local_connection = pymysql.connect(**self.config)
                local_cursor = local_connection.cursor()
                local_cursor.executemany(sql, batch_data)
                local_connection.commit()

                with lock:
                    progress_bar.update(len(batch_data))

                return True
            except Exception as e:
                logging.error(f"批次处理失败: {e}")
                if local_connection:
                    local_connection.rollback()
                return False
            finally:
                if local_cursor:
                    local_cursor.close()
                if local_connection:
                    local_connection.close()

        try:
            # 使用线程池执行任务
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}

                # 等待所有任务完成
                for future in as_completed(future_to_batch):
                    if not future.result():
                        is_success = False

        except Exception as e:
            logging.error(f"多线程执行失败: {e}")
            is_success = False
        finally:
            progress_bar.close()

        return is_success

    def batch_insert(self, table_name: str, columns: List[str],
                     data_list: List[Tuple[Any]], batch_size: int = 1000,
                     show_progress: bool = False, use_multithreading: bool = False,
                     max_workers: int = 4) -> bool:
        """
        批量插入数据

        Args:
            table_name: 表名
            columns: 列名列表
            data_list: 数据列表
            batch_size: 批量大小
            show_progress: 是否显示进度条
            use_multithreading: 是否使用多线程
            max_workers: 最大线程数

        Returns:
            bool: 插入是否成功
        """
        # 构造INSERT语句
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"

        return self.batch_execute(sql, data_list, batch_size, show_progress,
                                  use_multithreading, max_workers)

    def batch_update(self, table_name: str, set_columns: List[str],
                     where_column: str, data_list: List[Tuple[Any]],
                     batch_size: int = 1000, show_progress: bool = False,
                     use_multithreading: bool = False, max_workers: int = 4) -> bool:
        """
        批量更新数据

        Args:
            table_name: 表名
            set_columns: 需要更新的列名列表
            where_column: WHERE条件列名
            data_list: 数据列表，最后一个元素是WHERE条件值
            batch_size: 批量大小
            show_progress: 是否显示进度条
            use_multithreading: 是否使用多线程
            max_workers: 最大线程数

        Returns:
            bool: 更新是否成功
        """
        # 构造UPDATE语句
        set_clause = ', '.join([f"`{col}` = %s" for col in set_columns])
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE `{where_column}` = %s"

        return self.batch_execute(sql, data_list, batch_size, show_progress,
                                  use_multithreading, max_workers)

    def execute_query(self, sql: str, params: Optional[Tuple[Any]] = None) -> List[Tuple[Any]]:
        """
        执行查询语句

        Args:
            sql: 查询SQL语句
            params: 参数元组

        Returns:
            List[Tuple[Any]]: 查询结果
        """
        if not self.connection:
            if not self.connect():
                return []

        cursor = self.connection.cursor()
        result = []

        try:
            cursor.execute(sql, params or ())
            result = cursor.fetchall()
        except Exception as e:
            logging.error(f"查询执行失败: {e}")
        finally:
            cursor.close()

        return result


def generate_test_data(count: int) -> List[Tuple[Any]]:
    """
    生成测试数据

    Args:
        count: 数据条数

    Returns:
        List[Tuple[Any]]: 测试数据列表
    """
    data = []
    for i in range(count):
        # 生成随机用户名
        username = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        # 生成随机年龄(18-80)
        age = random.randint(18, 80)
        # 生成随机邮箱
        email = f"{username}@example.com"
        # 生成随机城市
        cities = ['北京', '上海', '广州', '深圳', '杭州', '南京', '武汉', '成都']
        city = random.choice(cities)
        data.append((username, age, email, city))
    return data


def create_test_table(processor: MySQLBatchProcessor):
    """
    创建测试表

    Args:
        processor: MySQLBatchProcessor实例
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        email VARCHAR(100) NOT NULL,
        city VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
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


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # 创建处理器实例
    processor = MySQLBatchProcessor(
        host='localhost',
        port=3306,
        user='root',
        password='123456',
        database='performance_db'
    )

    try:
        # 连接数据库
        if not processor.connect():
            print("数据库连接失败")
            exit(1)

        # 创建测试表
        create_test_table(processor)

        # 生成测试数据
        generate_data_count = 1000000
        print(f"正在生成{generate_data_count}条测试数据...")
        start_time = time.time()
        test_data = generate_test_data(generate_data_count)
        generate_time = time.time() - start_time
        print(f"数据生成完成，耗时: {generate_time:.2f}秒")

        # 批量插入数据并显示进度条
        print(f"开始插入{generate_data_count}条数据...")
        start_time = time.time()
        is_success = processor.batch_insert(
            table_name='test_users',
            columns=['username', 'age', 'email', 'city'],
            data_list=test_data,
            batch_size=5000,  # 调整批次大小
            show_progress=True,  # 显示进度条
            use_multithreading=True,  # 启用多线程
            max_workers=4  # 设置最大线程数
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
