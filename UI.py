import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
from MySQLScript import MySQLBatchProcessor, generate_test_data
import logging
import time


class MySQLBatchUI:
    def __init__(self, root):
        """
        初始化UI界面
        """
        self.about_frame = None
        self.root = root
        self.root.title("MySQL批量数据处理工具")
        self.root.geometry("800x600")

        # 数据库处理器实例
        self.processor = None

        # UI变量
        self.db_config_vars = {}
        self.data_config_vars = {}
        self.execution_vars = {}

        # 测试数据生成器映射配置
        self.test_data_generators = {
            'test_users': generate_test_data,
            # 可以在这里添加更多表名和对应的生成函数
            # 例如: 'orders': generate_order_data,
            #      'products': generate_product_data
        }

        # 创建界面
        self.create_widgets()

    def create_widgets(self):
        """
        创建所有UI组件
        """
        # 创建笔记本控件（标签页）
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 创建各个标签页
        self.create_connection_tab()
        self.create_data_generation_tab()
        self.create_execution_tab()
        self.create_monitor_tab()
        self.create_about_tab()

    def create_connection_tab(self):
        """
        创建数据库连接配置标签页
        """
        self.connection_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.connection_frame, text="数据库连接")

        # 连接参数输入区域
        config_frame = ttk.LabelFrame(self.connection_frame, text="连接配置")
        config_frame.pack(fill=tk.X, padx=10, pady=10)

        # 数据库配置变量
        config_fields = [
            ("主机地址", "host", "localhost"),
            ("端口", "port", "3306"),
            ("用户名", "user", "root"),
            ("密码", "password", ""),
            ("数据库名", "database", "performance_db"),
            ("字符集", "charset", "utf8mb4")
        ]

        for i, (label, key, default) in enumerate(config_fields):
            ttk.Label(config_frame, text=label + ":").grid(row=i, column=0, sticky=tk.W, padx=5, pady=2)
            var = tk.StringVar(value=default)
            self.db_config_vars[key] = var
            entry = ttk.Entry(config_frame, textvariable=var, width=30)
            if key == "password":
                entry.config(show="*")
            entry.grid(row=i, column=1, padx=5, pady=2, sticky=tk.EW)

        config_frame.columnconfigure(1, weight=1)

        # 按钮区域
        button_frame = ttk.Frame(self.connection_frame)
        button_frame.pack(fill=tk.X, padx=10, pady=10)

        self.test_conn_btn = ttk.Button(button_frame, text="测试连接", command=self.test_connection)
        self.test_conn_btn.pack(side=tk.LEFT, padx=5)

        self.connect_btn = ttk.Button(button_frame, text="建立连接", command=self.connect_database)
        self.connect_btn.pack(side=tk.LEFT, padx=5)

        self.disconnect_btn = ttk.Button(button_frame, text="断开连接", command=self.disconnect_database,
                                         state=tk.DISABLED)
        self.disconnect_btn.pack(side=tk.LEFT, padx=5)

    def create_data_generation_tab(self):
        """
        创建数据生成配置标签页
        """
        self.data_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.data_frame, text="数据生成")

        # 数据生成配置区域
        data_config_frame = ttk.LabelFrame(self.data_frame, text="生成配置")
        data_config_frame.pack(fill=tk.X, padx=10, pady=10)

        # 数据配置变量
        data_fields = [
            ("生成数据量", "data_count", "10000"),
            ("批次大小", "batch_size", "1000"),
            ("最大线程数", "max_workers", "4")
        ]

        self.data_config_vars = {}
        for i, (label, key, default) in enumerate(data_fields):
            ttk.Label(data_config_frame, text=label + ":").grid(row=i, column=0, sticky=tk.W, padx=5, pady=2)
            var = tk.StringVar(value=default)
            self.data_config_vars[key] = var
            entry = ttk.Entry(data_config_frame, textvariable=var, width=20)
            entry.grid(row=i, column=1, padx=5, pady=2, sticky=tk.W)

        # 表名配置
        ttk.Label(data_config_frame, text="表名:").grid(row=len(data_fields), column=0, sticky=tk.W, padx=5, pady=2)
        self.table_name_var = tk.StringVar(value="test_users")
        table_entry = ttk.Entry(data_config_frame, textvariable=self.table_name_var, width=20)
        table_entry.grid(row=len(data_fields), column=1, padx=5, pady=2, sticky=tk.W)

        # 列名配置
        ttk.Label(data_config_frame, text="列名 (逗号分隔):").grid(row=len(data_fields) + 1, column=0, sticky=tk.W,
                                                                   padx=5, pady=2)
        self.columns_var = tk.StringVar(value="username,age,email,city")
        columns_entry = ttk.Entry(data_config_frame, textvariable=self.columns_var, width=50)
        columns_entry.grid(row=len(data_fields) + 1, column=1, padx=5, pady=2, sticky=tk.W)

        # 选项配置
        options_frame = ttk.LabelFrame(self.data_frame, text="选项")
        options_frame.pack(fill=tk.X, padx=10, pady=10)

        self.show_progress_var = tk.BooleanVar(value=True)
        self.use_multithreading_var = tk.BooleanVar(value=True)
        self.auto_optimize_var = tk.BooleanVar(value=True)

        ttk.Checkbutton(options_frame, text="显示进度条", variable=self.show_progress_var).pack(anchor=tk.W, padx=5,
                                                                                                pady=2)
        ttk.Checkbutton(options_frame, text="启用多线程", variable=self.use_multithreading_var).pack(anchor=tk.W,
                                                                                                     padx=5, pady=2)
        ttk.Checkbutton(options_frame, text="自动优化", variable=self.auto_optimize_var).pack(anchor=tk.W, padx=5,
                                                                                              pady=2)

    def create_execution_tab(self):
        """
        创建执行方式标签页
        """
        self.execution_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.execution_frame, text="执行方式")

        # 执行方式选择
        mode_frame = ttk.LabelFrame(self.execution_frame, text="导入方式")
        mode_frame.pack(fill=tk.X, padx=10, pady=10)

        self.execution_mode_var = tk.StringVar(value="batch_insert")

        modes = [
            ("多线程批量插入", "batch_insert"),
            ("LOAD DATA INFILE", "load_data_infile"),
            ("生成SQL脚本文件", "generate_sql_script"),
            ("生成CSV文件", "generate_csv_file")
        ]

        for i, (text, value) in enumerate(modes):
            ttk.Radiobutton(mode_frame, text=text, variable=self.execution_mode_var,
                            value=value).pack(anchor=tk.W, padx=5, pady=2)

        # 文件路径选择（用于SQL脚本和CSV文件生成）
        file_frame = ttk.LabelFrame(self.execution_frame, text="文件配置")
        file_frame.pack(fill=tk.X, padx=10, pady=10)

        ttk.Label(file_frame, text="输出文件路径:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.output_path_var = tk.StringVar()
        path_entry = ttk.Entry(file_frame, textvariable=self.output_path_var, width=50)
        path_entry.grid(row=0, column=1, padx=5, pady=2, sticky=tk.EW)

        browse_btn = ttk.Button(file_frame, text="浏览...", command=self.browse_output_path)
        browse_btn.grid(row=0, column=2, padx=5, pady=2)

        file_frame.columnconfigure(1, weight=1)

        # 执行按钮
        execute_frame = ttk.Frame(self.execution_frame)
        execute_frame.pack(fill=tk.X, padx=10, pady=10)

        self.execute_btn = ttk.Button(execute_frame, text="开始执行", command=self.start_execution)
        self.execute_btn.pack(pady=10)

    def create_monitor_tab(self):
        """
        创建监控标签页
        """
        self.monitor_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.monitor_frame, text="执行监控")

        # 日志显示区域
        log_frame = ttk.LabelFrame(self.monitor_frame, text="执行日志")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 创建文本框和滚动条
        self.log_text = tk.Text(log_frame, height=20, state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)

        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y, pady=5)

        # 清除日志按钮
        clear_btn = ttk.Button(self.monitor_frame, text="清除日志", command=self.clear_log)
        clear_btn.pack(pady=5)

    def create_about_tab(self):
        """
        创建关于标签页
        """
        self.about_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.about_frame, text="关于")

        # 创建一个文本框来显示版本信息和声明
        about_text = tk.Text(self.about_frame, wrap=tk.WORD, state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(self.about_frame, orient=tk.VERTICAL, command=about_text.yview)
        about_text.configure(yscrollcommand=scrollbar.set)

        about_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 10), pady=10)

        # 插入版本信息和声明
        about_content = """
    1. 测试数据生成
       - 表名自动匹配测试数据生成函数（init里配表和其对应的生成数据函数）

    2. 执行方式
       - 多线程批量插入
       - LOAD DATA INFILE (还没实现)
       - SQL脚本文件生成 (还没实现)
       - CSV文件生成 (还没实现)

    使用说明:
    1. 在"数据库连接"标签页配置数据库连接参数并建立连接
    2. 在"数据生成"标签页配置数据生成参数
    3. 在"执行方式"标签页选择执行方式
    4. 点击"开始执行"按钮启动操作
    5. 在"执行监控"标签页查看执行日志
    """

        about_text.config(state=tk.NORMAL)
        about_text.insert(tk.END, about_content)
        about_text.config(state=tk.DISABLED)

    def test_connection(self):
        """
        测试数据库连接
        """
        try:
            config = self.get_db_config()
            processor = MySQLBatchProcessor(**config)
            if processor.connect():
                processor.disconnect()
                messagebox.showinfo("连接测试", "数据库连接成功！")
            else:
                messagebox.showerror("连接测试", "数据库连接失败！")
        except Exception as e:
            messagebox.showerror("连接测试", f"连接测试出错：{str(e)}")

    def connect_database(self):
        """
        建立数据库连接
        """
        try:
            config = self.get_db_config()
            config['auto_optimize'] = self.auto_optimize_var.get()
            self.processor = MySQLBatchProcessor(**config)
            if self.processor.connect():
                self.connect_btn.config(state=tk.DISABLED)
                self.disconnect_btn.config(state=tk.NORMAL)
                self.log_message("数据库连接成功")
                messagebox.showinfo("连接", "数据库连接成功！")
            else:
                messagebox.showerror("连接", "数据库连接失败！")
        except Exception as e:
            messagebox.showerror("连接", f"连接出错：{str(e)}")

    def disconnect_database(self):
        """
        断开数据库连接
        """
        if self.processor:
            self.processor.disconnect()
            self.processor = None
            self.connect_btn.config(state=tk.NORMAL)
            self.disconnect_btn.config(state=tk.DISABLED)
            self.log_message("数据库连接已断开")
            messagebox.showinfo("断开连接", "数据库连接已断开！")

    def start_execution(self):
        """
        开始执行批量操作
        """
        if not self.processor:
            messagebox.showwarning("执行", "请先建立数据库连接！")
            return

        # 在新线程中执行，避免阻塞UI
        execution_thread = threading.Thread(target=self.execute_batch_operation)
        execution_thread.daemon = True
        execution_thread.start()

    def execute_batch_operation(self):
        """
        执行批量操作（在后台线程中运行）
        """
        try:
            mode = self.execution_mode_var.get()

            if mode == "batch_insert":
                self.execute_batch_insert()
            elif mode == "load_data_infile":
                self.execute_load_data_infile()
            elif mode == "generate_sql_script":
                self.execute_generate_sql_script()
            elif mode == "generate_csv_file":
                self.execute_generate_csv_file()

        except Exception as e:
            self.log_message(f"执行出错：{str(e)}", level="ERROR")

    def execute_batch_insert(self):
        """
        执行批量插入
        """
        try:
            # 获取配置参数
            data_count = int(self.data_config_vars['data_count'].get())
            batch_size = int(self.data_config_vars['batch_size'].get())
            max_workers = int(self.data_config_vars['max_workers'].get())
            table_name = self.table_name_var.get()
            columns = [col.strip() for col in self.columns_var.get().split(',')]
            show_progress = self.show_progress_var.get()
            use_multithreading = self.use_multithreading_var.get()

            self.log_message(f"开始执行批量插入: {data_count} 条数据")
            self.log_message(f"配置信息 - 表名: {table_name}, 列名: {columns}")
            self.log_message(f"执行参数 - 批次大小: {batch_size}, 线程数: {max_workers}")

            # 检查是否有对应的测试数据生成函数
            if table_name not in self.test_data_generators:
                self.log_message(f"错误: 表 '{table_name}' 没有对应的测试数据生成函数，操作已停止", level="ERROR")
                return

            generator_func = self.test_data_generators[table_name]

            # 生成测试数据
            self.log_message("正在生成测试数据...")
            test_data = generator_func(data_count)

            if not test_data:
                self.log_message("数据生成失败", level="ERROR")
                return

            self.log_message(f"数据生成完成，共 {len(test_data)} 条记录")

            # 执行批量插入
            self.log_message("开始批量插入数据...")
            start_time = time.time()

            is_success = self.processor.batch_insert(
                table_name=table_name,
                columns=columns,
                data_list=test_data,
                batch_size=batch_size,
                show_progress=show_progress,
                use_multithreading=use_multithreading,
                max_workers=max_workers
            )

            end_time = time.time()
            elapsed_time = end_time - start_time

            if is_success:
                self.log_message(f"批量插入成功完成!")
                self.log_message(f"总耗时: {elapsed_time:.2f} 秒")
                self.log_message(f"平均速度: {len(test_data) / elapsed_time:.0f} 条/秒")
            else:
                self.log_message("批量插入失败", level="ERROR")

        except Exception as e:
            self.log_message(f"批量插入执行出错: {str(e)}", level="ERROR")

    def execute_load_data_infile(self):
        """
        执行LOAD DATA INFILE
        """
        # TODO: 实现LOAD DATA INFILE逻辑
        self.log_message("开始执行LOAD DATA INFILE...")

    def execute_generate_sql_script(self):
        """
        执行生成SQL脚本
        """
        # TODO: 实现生成SQL脚本逻辑
        self.log_message("开始生成SQL脚本...")

    def execute_generate_csv_file(self):
        """
        执行生成CSV文件
        """
        # TODO: 实现生成CSV文件逻辑
        self.log_message("开始生成CSV文件...")

    def get_db_config(self):
        """
        获取数据库配置
        """
        return {
            'host': self.db_config_vars['host'].get(),
            'port': int(self.db_config_vars['port'].get()),
            'user': self.db_config_vars['user'].get(),
            'password': self.db_config_vars['password'].get(),
            'database': self.db_config_vars['database'].get(),
            'charset': self.db_config_vars['charset'].get()
        }

    def browse_output_path(self):
        """
        浏览输出文件路径
        """
        file_path = filedialog.asksaveasfilename(
            title="选择输出文件",
            filetypes=[
                ("SQL files", "*.sql"),
                ("CSV files", "*.csv"),
                ("All files", "*.*")
            ]
        )
        if file_path:
            self.output_path_var.set(file_path)

    def log_message(self, message, level="INFO"):
        """
        记录日志消息
        """
        # 使用after方法确保在主线程中更新UI
        self.root.after(0, self._update_log_display, f"[{level}] {message}")

    def _update_log_display(self, message):
        """
        更新日志显示（在主线程中调用）
        """
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)

    def clear_log(self):
        """
        清除日志
        """
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)


def main():
    """
    主函数
    """
    # 配置日志
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # 创建主窗口
    root = tk.Tk()
    app = MySQLBatchUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
