from MySQLScript import MySQLBatchProcessor
from typing import List, Tuple, Any, Optional
import logging
import random
import string
import re
from datetime import datetime
import uuid

# 仅测试类，不够通用
class MultiTableDataGenerator:
    """多表关联数据生成器"""

    def __init__(self, processor: MySQLBatchProcessor):
        self.processor = processor
        self.id_mappings = {}

    def generate_related_data(self, schema_config: dict, record_counts: dict) -> dict:
        """
        生成具有关联关系的多表数据

        Args:
            schema_config: 数据库表结构配置
            record_counts: 每个表需要生成的记录数量

        Returns:
            dict: 生成的数据，键为表名，值为数据列表
        """
        generated_data = {}

        # 按依赖顺序生成数据（确保父表数据先于子表生成）
        ordered_tables = self._get_ordered_tables(schema_config)

        for table_name in ordered_tables:
            if table_name in record_counts:
                table_data = self._generate_table_data(
                    table_name,
                    schema_config[table_name],
                    record_counts[table_name],
                    generated_data  # 传入已生成的数据，用于外键引用
                )
                generated_data[table_name] = table_data

        return generated_data

    def _get_ordered_tables(self, schema_config: dict) -> List[str]:
        """获取按依赖关系排序的表名列表"""
        dependencies = {}

        for table_name, table_info in schema_config.items():
            dependencies[table_name] = []
            if 'foreign_keys' in table_info:
                for fk in table_info['foreign_keys']:
                    dependencies[table_name].append(fk['references_table'])

        # 拓扑排序
        visited = set()
        order = []

        def dfs(table_name):
            if table_name in visited:
                return
            visited.add(table_name)

            for dep in dependencies.get(table_name, []):
                dfs(dep)

            order.append(table_name)

        for table_name in dependencies:
            dfs(table_name)

        return order

    def _generate_table_data(self, table_name: str, table_schema: dict,
                             count: int, existing_data: dict) -> List[Tuple]:
        """生成单个表的数据"""
        data = []

        for i in range(count):
            row_data = []

            for column in table_schema['columns']:
                col_name = column['name']

                # 检查是否为 ID 列，如果是则生成 UUID
                if 'id' in col_name.lower() or 'AUTO_INCREMENT' in column.get('extra', ''):
                    row_data.append(str(uuid.uuid4()).replace('-', ''))  # 生成 UUID 作为 ID
                    continue

                # 检查是否为外键
                is_foreign_key = False
                if 'foreign_keys' in table_schema:
                    for fk in table_schema['foreign_keys']:
                        if fk['column'] == col_name:
                            # 从父表获取ID
                            parent_table = fk['references_table']
                            if parent_table in existing_data and existing_data[parent_table]:
                                # 从父表已生成的ID中随机选择一个
                                parent_ids = [row[0] for row in existing_data[parent_table] if row[0] is not None]
                                if parent_ids:
                                    row_data.append(random.choice(parent_ids))
                                else:
                                    # 如果父表没有ID，生成一个UUID
                                    row_data.append(str(uuid.uuid4()).replace('-', ''))
                            else:
                                # 如果父表还没有数据，生成一个UUID
                                row_data.append(str(uuid.uuid4()).replace('-', ''))
                            is_foreign_key = True
                            break

                if not is_foreign_key:
                    # 生成普通列数据
                    row_data.append(self._generate_column_value(column))

            data.append(tuple(row_data))

        return data

    def _generate_column_value(self, column_schema: dict) -> Any:
        """根据列类型生成值"""
        col_type = column_schema['type'].upper()

        if 'id' in column_schema['name'].lower() or col_type == 'INT' and 'AUTO_INCREMENT' in column_schema.get('extra',
                                                                                                                ''):
            return str(uuid.uuid4()).replace('-', '')  # ID字段使用UUID
        elif col_type.startswith('VARCHAR') or col_type == 'TEXT':
            length = int(re.search(r'\d+', col_type).group()) if re.search(r'\d+', col_type) else 50
            return ''.join(random.choices(string.ascii_letters + string.digits, k=min(length, 8)))
        elif col_type == 'INT':
            return random.randint(1, 1000)
        elif col_type.startswith('DECIMAL') or col_type.startswith('FLOAT') or col_type.startswith('DOUBLE'):
            # 处理 DECIMAL 类型
            return round(random.uniform(1.0, 9999.99), 2)
        elif col_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elif col_type == 'BOOLEAN':
            return random.choice([True, False])
        else:
            # 为未知类型提供默认值，基于列名推断
            col_name_lower = column_schema['name'].lower()
            if any(keyword in col_name_lower for keyword in ['amount', 'price', 'cost', 'total', 'sum', 'value']):
                # 金额相关列返回数值
                return round(random.uniform(1.0, 9999.99), 2)
            elif any(keyword in col_name_lower for keyword in ['name', 'title', 'desc', 'comment']):
                # 名称相关列返回字符串
                return ''.join(random.choices(string.ascii_letters, k=8))
            elif any(keyword in col_name_lower for keyword in ['date', 'time']):
                # 日期相关列返回日期
                return datetime.now().strftime('%Y-%m-%d')
            elif any(keyword in col_name_lower for keyword in ['id', 'code', 'num']):
                # ID/编码相关列返回数值
                return random.randint(1, 1000)
            else:
                # 对于其他情况，生成字符串，但避免使用 "sample_" 前缀造成数值列错误
                return ''.join(random.choices(string.ascii_letters, k=8))

    def insert_related_data(self, data_dict: dict, schema_config: dict, batch_size: int = 1000) -> bool:
        """插入多表关联数据"""
        try:
            for table_name, data_list in data_dict.items():
                if data_list:
                    # 获取所有列名
                    columns = [col['name'] for col in schema_config[table_name]['columns']]

                    # 验证数据与列是否匹配
                    for row in data_list:
                        if len(row) != len(columns):
                            logging.error(
                                f"数据行长度与列数不匹配: 表={table_name}, 行长度={len(row)}, 列数={len(columns)}, 行数据={row}")
                            return False

                    success = self.processor.batch_insert(
                        table_name=table_name,
                        columns=columns,
                        data_list=data_list,
                        batch_size=batch_size,
                        show_progress=True
                    )

                    if not success:
                        logging.error(f"插入表 {table_name} 数据失败")
                        return False

            logging.info("所有表数据插入成功")
            return True

        except Exception as e:
            logging.error(f"插入多表数据失败: {e}")
            return False

